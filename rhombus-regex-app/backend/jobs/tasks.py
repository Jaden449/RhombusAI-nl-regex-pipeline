import logging
import shutil

from celery import shared_task
from celery.exceptions import Ignore, SoftTimeLimitExceeded
from django.conf import settings

from .llm import generate_regex
from .models import Job
from .regex_utils import RegexValidationError
from .spark_engine import run_replacement

logger = logging.getLogger(__name__)


@shared_task(
    bind=True,
    autoretry_for=(ConnectionError,),  # transient infra errors (e.g. Spark/Redis hiccups)
    retry_backoff=True,
    retry_backoff_max=60,
    retry_kwargs={"max_retries": 3},
)
def process_job(self, job_id: str):
    try:
        job = Job.objects.get(id=job_id)
    except Job.DoesNotExist:
        logger.error("Job %s vanished before processing started", job_id)
        raise Ignore()

    # A cancellation request sets status to CANCELLED before the worker
    # picks the task up -- honor it instead of doing wasted work.
    job.refresh_from_db()
    if job.status == Job.Status.CANCELLED:
        return

    job.status = Job.Status.RUNNING
    job.celery_task_id = self.request.id
    job.progress = 1
    job.save(update_fields=["status", "celery_task_id", "progress"])

    def report_progress(pct: int):
        # cheap re-check so a cancel mid-run stops future progress writes
        Job.objects.filter(id=job_id).update(progress=pct)
        self.update_state(state="PROGRESS", meta={"progress": pct})

    try:
        pattern, source = generate_regex(job.nl_prompt)
        job.regex_pattern = pattern
        job.regex_source = source
        job.progress = 5
        job.save(update_fields=["regex_pattern", "regex_source", "progress"])

        output_dir = str(settings.RESULTS_ROOT / str(job.id))
        result = run_replacement(
            file_path=job.input_file.path,
            target_column=job.target_column,
            pattern=pattern,
            replacement_value=job.replacement_value,
            output_dir=output_dir,
            progress_callback=report_progress,
        )

        job.refresh_from_db(fields=["status"])
        if job.status == Job.Status.CANCELLED:
            shutil.rmtree(output_dir, ignore_errors=True)
            return

        job.status = Job.Status.SUCCESS
        job.progress = 100
        job.result_dir = result.result_dir
        job.row_count = result.row_count
        job.matched_count = result.matched_count
        job.save()

    except RegexValidationError as exc:
        job.status = Job.Status.FAILED
        job.error_message = f"Regex rejected: {exc}"
        job.save(update_fields=["status", "error_message"])
        # not retried: a bad prompt/pattern won't fix itself on retry

    except SoftTimeLimitExceeded:
        job.status = Job.Status.FAILED
        job.error_message = "Job exceeded the time limit and was terminated."
        job.save(update_fields=["status", "error_message"])
        raise

    except Exception as exc:  # noqa: BLE001
        logger.exception("Job %s failed", job_id)
        job.status = Job.Status.FAILED
        job.error_message = str(exc)
        job.save(update_fields=["status", "error_message"])
        raise
