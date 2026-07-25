from django.urls import path

from . import views

urlpatterns = [
    path("jobs/", views.JobListCreateView.as_view(), name="job-list-create"),
    path("jobs/<uuid:job_id>/status/", views.JobStatusView.as_view(), name="job-status"),
    path("jobs/<uuid:job_id>/result/", views.JobResultView.as_view(), name="job-result"),
    path("jobs/<uuid:job_id>/cancel/", views.JobCancelView.as_view(), name="job-cancel"),
]
