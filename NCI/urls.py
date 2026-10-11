
from django.contrib import admin
from django.urls import path, include
from .views import NCIAnalysisAPIView, NCIRcaSitesAPIView


urlpatterns = [
 path("analysis/", NCIAnalysisAPIView.as_view(), name="nci-analysis"),
 path("analysis/<str:job_id>/", NCIAnalysisAPIView.as_view(), name="nci-analysis-status"),
 path("rca/", NCIRcaSitesAPIView.as_view(), name="nci-rca-sites"),
 path("rca/<str:job_id>/", NCIRcaSitesAPIView.as_view(), name="nci-rca-sites-status"),
]


