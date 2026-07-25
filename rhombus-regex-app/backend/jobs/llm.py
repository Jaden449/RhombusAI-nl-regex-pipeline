"""
Turns a natural-language description ("find email addresses") into a regex
pattern, using Claude. Results are cached in Redis keyed by a hash of the
prompt so identical requests never hit the LLM twice.
"""

import hashlib
import json
import logging

import redis
from django.conf import settings

from .regex_utils import RegexValidationError, validate_and_compile

logger = logging.getLogger(__name__)

_redis_client = redis.Redis.from_url(settings.REGEX_CACHE_REDIS_URL, decode_responses=True)

CACHE_TTL_SECONDS = 60 * 60 * 24 * 30  # 30 days -- prompts don't go stale

SYSTEM_PROMPT = (
    "You convert a natural-language description of a text pattern into a single "
    "Python-flavored regular expression. Reply with ONLY the raw regex pattern "
    "and nothing else: no backticks, no explanation, no quotes around it, no "
    "'Sure, here is...'. If the request is ambiguous, pick the most common-sense "
    "interpretation. The pattern must be safe (avoid nested quantifiers like "
    "(a+)+ that cause catastrophic backtracking)."
)

# A small set of well-known patterns used when no ANTHROPIC_API_KEY is
# configured, so the whole pipeline is still demoable offline.
_FALLBACK_LIBRARY = {
    "email": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,7}\b",
    "phone": r"\b(?:\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b",
    "url": r"\bhttps?://[^\s]+\b",
    "date": r"\b\d{4}-\d{2}-\d{2}\b",
    "zip": r"\b\d{5}(?:-\d{4})?\b",
    "ipv4": r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
    "hashtag": r"#\w+",
    "currency": r"\$\d+(?:,\d{3})*(?:\.\d{2})?",
}


def _cache_key(prompt: str) -> str:
    digest = hashlib.sha256(prompt.strip().lower().encode("utf-8")).hexdigest()
    return f"regex_cache:{digest}"


def _fallback_regex(prompt: str) -> str:
    lowered = prompt.lower()
    for keyword, pattern in _FALLBACK_LIBRARY.items():
        if keyword in lowered:
            return pattern
    # last resort: escape the literal prompt as a substring match
    raise RegexValidationError(
        "No ANTHROPIC_API_KEY configured and no fallback pattern matched this "
        "request. Set ANTHROPIC_API_KEY in your .env, or try a prompt like "
        "'find email addresses'."
    )


def _call_llm(prompt: str) -> str:
    if not settings.ANTHROPIC_API_KEY:
        return _fallback_regex(prompt)

    import anthropic

    client = anthropic.Anthropic(api_key=settings.ANTHROPIC_API_KEY)
    message = client.messages.create(
        model=settings.LLM_MODEL,
        max_tokens=200,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    text_blocks = [b.text for b in message.content if b.type == "text"]
    raw = "".join(text_blocks).strip()
    # strip accidental markdown fences just in case
    raw = raw.strip("`").strip()
    if raw.lower().startswith("regex:"):
        raw = raw.split(":", 1)[1].strip()
    return raw


def generate_regex(prompt: str) -> tuple[str, str]:
    """
    Returns (pattern, source) where source is one of "cache", "llm", "fallback".
    Raises RegexValidationError if nothing safe/valid could be produced.
    """
    key = _cache_key(prompt)
    cached = _redis_client.get(key)
    if cached:
        data = json.loads(cached)
        return data["pattern"], "cache"

    source = "llm" if settings.ANTHROPIC_API_KEY else "fallback"
    pattern = _call_llm(prompt)
    pattern = validate_and_compile(pattern)  # raises if invalid/unsafe

    _redis_client.setex(key, CACHE_TTL_SECONDS, json.dumps({"pattern": pattern}))
    return pattern, source
