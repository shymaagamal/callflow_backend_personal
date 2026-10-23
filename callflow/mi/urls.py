from django.urls import path

from callflow.mi import views

urlpatterns = [
    path("smartcare", views.MISmartcareView.as_view(), name="mi-smartcare"),
    path("smartcare/jobs/<str:job_id>", views.JobStatusView.as_view(), name="mi-smartcare-job"),
    path("smartcare/jobs/<str:job_id>/result", views.JobResultView.as_view(), name="mi-smartcare-job-result"),
]

# Served under /callflow/prototype/api/ (old path, old synchronous contract by default)
legacy_urlpatterns = [
    path("smartcare", views.LegacySmartcareView.as_view(), name="smartcare-api"),
    path("smartcare/jobs/<str:job_id>", views.JobStatusView.as_view(), name="prototype-smartcare-job"),
    path("smartcare/jobs/<str:job_id>/result", views.JobResultView.as_view(),
         name="prototype-smartcare-job-result"),
]
