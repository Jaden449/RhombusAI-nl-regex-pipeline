import duckdb
from celery.result import AsyncResult
from django.shortcuts import get_object_or_404
from rest_framework import status
from rest_framework.response import Response
from rest_framework.views import APIView

from .models import Job
from .serializers import JobCreateSerializer, JobStatusSerializer
from .tasks import process_job


class JobListCreateView(APIView):
    def post(self, request):
        serializer = JobCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        job = serializer.save(
            original_filename=request.data.get("input_file").name,
            status=Job.Status.QUEUED,
        )
        process_job.delay(str(job.id))
        return Response(JobStatusSerializer(job).data, status=status.HTTP_201_CREATED)

    def get(self, request):
        jobs = Job.objects.all()[:50]
        return Response(JobStatusSerializer(jobs, many=True).data)


class JobStatusView(APIView):
    def get(self, request, job_id):
        job = get_object_or_404(Job, id=job_id)
        return Response(JobStatusSerializer(job).data)


class JobResultView(APIView):
    """
    Paginated result reader. Reads directly from the Parquet part-files
    Spark wrote out, using DuckDB's zero-copy Parquet scan -- so a page of
    50 rows out of a 5-million-row result costs a cheap columnar seek, not a
    full load into Django's memory.
    """

    def get(self, request, job_id):
        job = get_object_or_404(Job, id=job_id)
        if job.status != Job.Status.SUCCESS:
            return Response(
                {"detail": f"Job is not ready yet (status={job.status})."},
                status=status.HTTP_409_CONFLICT,
            )

        page = max(1, int(request.query_params.get("page", 1)))
        page_size = min(500, max(1, int(request.query_params.get("page_size", 50))))
        offset = (page - 1) * page_size

        glob_path = f"{job.result_dir}/*.parquet"
        con = duckdb.connect()
        rows = con.execute(
            f"SELECT * FROM read_parquet('{glob_path}') LIMIT ? OFFSET ?",
            [page_size, offset],
        ).fetchdf()

        return Response(
            {
                "page": page,
                "page_size": page_size,
                "row_count": job.row_count,
                "matched_count": job.matched_count,
                "total_pages": (job.row_count // page_size) + 1 if job.row_count else 0,
                "columns": list(rows.columns),
                "rows": rows.to_dict(orient="records"),
            }
        )


class JobCancelView(APIView):
    def post(self, request, job_id):
        job = get_object_or_404(Job, id=job_id)
        if job.status in (Job.Status.SUCCESS, Job.Status.FAILED, Job.Status.CANCELLED):
            return Response(
                {"detail": f"Job already finished (status={job.status})."},
                status=status.HTTP_409_CONFLICT,
            )

        job.status = Job.Status.CANCELLED
        job.save(update_fields=["status"])

        if job.celery_task_id:
            AsyncResult(job.celery_task_id).revoke(terminate=True, signal="SIGTERM")

        return Response(JobStatusSerializer(job).data)
