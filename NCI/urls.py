
from django.contrib import admin
from django.urls import path, include
from .views import NCIAnalysisAPIView, NCIRcaSitesAPIView, NCIPortalAnalysisAPIView


urlpatterns = [
 path("analysis/", NCIAnalysisAPIView.as_view(), name="nci-analysis"),
 path("analysis/<str:job_id>/", NCIAnalysisAPIView.as_view(), name="nci-analysis-status"),
 path("rca/", NCIRcaSitesAPIView.as_view(), name="nci-rca-sites"),
 path("rca/<str:job_id>/", NCIRcaSitesAPIView.as_view(), name="nci-rca-sites-status"),
 # Portal view of an NCI analysis (the portal_url in the NCI response opens it).
 # The portal calls these without a trailing slash; both forms are accepted.
 path("portal/analysis", NCIPortalAnalysisAPIView.as_view(), name="nci-portal-analysis"),
 path("portal/analysis/", NCIPortalAnalysisAPIView.as_view()),
 path("portal/analysis/<str:nci_id>", NCIPortalAnalysisAPIView.as_view(), name="nci-portal-analysis-result"),
 path("portal/analysis/<str:nci_id>/", NCIPortalAnalysisAPIView.as_view()),
]


