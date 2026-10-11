
from django.contrib import admin
from django.urls import path, include
from .views import NCIAnalysisAPIView


urlpatterns = [
 path("analysis/", NCIAnalysisAPIView.as_view(), name="nci-analysis"),
 path("analysis/<str:job_id>/", NCIAnalysisAPIView.as_view(), name="nci-analysis-status"),
]


