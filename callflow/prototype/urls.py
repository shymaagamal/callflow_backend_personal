from django.urls import path
from callflow.prototype import views

urlpatterns = [
    path("smartcare", views.PrototypeSmartcareIntegrationAPIView.as_view(), name="smartcare-api"),
    path("smartcare/jobs/<str:job_id>", views.PrototypeSmartcareJobStatusAPIView.as_view(),
         name="prototype-smartcare-job"),
]
