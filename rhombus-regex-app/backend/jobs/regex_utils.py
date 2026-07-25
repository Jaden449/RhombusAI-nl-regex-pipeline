"""
Validates LLM-generated regex patterns before they are ever applied to real data.

Two layers of defense:
1. Syntax check -- does it even compile as a Python regex?
2. Runtime safety check -- does it explode (catastrophic backtracking) on
   adversarial input? We can't *prove* a pattern is safe in general (that's
   equivalent to solving the halting problem for regex engines), so instead
   we run the pattern against a small battery of known ReDoS-triggering
   strings inside an isolated subprocess with a hard wall-clock timeout.
   If it can't finish fast on any of them, we reject it. This is the same
   practical approach used by several open-source ReDoS scanners.
"""

import re

import billiard as multiprocessing

# Strings engineered to blow up classic vulnerable patterns like
# (a+)+$, (a|a)+$, (a|aa)+$, etc. against non-matching input, which is the
# textbook catastrophic-backtracking trigger.
_ADVERSARIAL_INPUTS = [
    "a" * 25 + "!",
    "a" * 35 + "!",
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaX",
    ("ab" * 20) + "!",
    "0123456789" * 6 + "X",
]

MAX_PATTERN_LENGTH = 500
PER_TEST_TIMEOUT_SECONDS = 1.0


class RegexValidationError(ValueError):
    pass


def _match_worker(pattern: str, text: str, queue: multiprocessing.Queue):
    try:
        compiled = re.compile(pattern)
        compiled.search(text)
        queue.put("ok")
    except Exception as exc:  # noqa: BLE001
        queue.put(f"error:{exc}")


def _runs_fast_enough(pattern: str, text: str) -> bool:
    queue: multiprocessing.Queue = multiprocessing.Queue()
    proc = multiprocessing.Process(target=_match_worker, args=(pattern, text, queue))
    proc.start()
    proc.join(PER_TEST_TIMEOUT_SECONDS)
    if proc.is_alive():
        proc.terminate()
        proc.join()
        return False
    return True


def validate_and_compile(pattern: str) -> str:
    """
    Raises RegexValidationError if the pattern is invalid or unsafe.
    Returns the (unchanged) pattern string if it passes.
    """
    if not pattern or not pattern.strip():
        raise RegexValidationError("Empty regex pattern.")

    pattern = pattern.strip()

    if len(pattern) > MAX_PATTERN_LENGTH:
        raise RegexValidationError("Generated pattern is unreasonably long; refusing to apply it.")

    # 1. Syntax check
    try:
        re.compile(pattern)
    except re.error as exc:
        raise RegexValidationError(f"Invalid regex syntax: {exc}") from exc

    # 2. Runtime / ReDoS check against adversarial inputs
    for text in _ADVERSARIAL_INPUTS:
        if not _runs_fast_enough(pattern, text):
            raise RegexValidationError(
                "Pattern failed the catastrophic-backtracking safety check "
                "(took too long against adversarial input) and was rejected."
            )

    return pattern
