import uuid

from django.db import models


def upload_path(instance, filename):
    return f"uploads/{instance.id}/{filename}"


class Job(models.Model):
    class Status(models.TextChoices):
        QUEUED = "QUEUED", "Queued"
        RUNNING = "RUNNING", "Running"
        SUCCESS = "SUCCESS", "Success"
        FAILED = "FAILED", "Failed"
        CANCELLED = "CANCELLED", "Cancelled"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    input_file = models.FileField(upload_to=upload_path)
    original_filename = models.CharField(max_length=512)

    # what the user asked for
    nl_prompt = models.TextField(help_text="Natural language description of the pattern")
    target_column = models.CharField(max_length=255)
    replacement_value = models.TextField(blank=True, default="")

    # what the LLM produced
    regex_pattern = models.CharField(max_length=2000, blank=True, default="")
    regex_source = models.CharField(
        max_length=20,
        choices=[("llm", "llm"), ("cache", "cache"), ("fallback", "fallback")],
        blank=True,
        default="",
    )

    # execution state
    status = models.CharField(max_length=12, choices=Status.choices, default=Status.QUEUED)
    progress = models.PositiveSmallIntegerField(default=0)  # 0-100
    celery_task_id = models.CharField(max_length=255, blank=True, default="")
    error_message = models.TextField(blank=True, default="")

    # results
    result_dir = models.CharField(max_length=1024, blank=True, default="")
    row_count = models.BigIntegerField(null=True, blank=True)
    matched_count = models.BigIntegerField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        return f"Job({self.id}) [{self.status}] {self.original_filename}"
