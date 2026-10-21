# callflow/core/urls_patch.py
"""
Full url patterns for the job-backed pattern, covering every system.
Replace the relevant lines in your existing urls.py with these.

Every POST endpoint now returns {job_id, status: "PENDING"} immediately.
Every system gets a matching GET <job_id> route for polling.
"""

from django.urls import path


from .views.test_npp_kpi import NppKpiAPIView

from .views.smartcare_site_kpi import SmartcareSiteKpiAPIView
# from .views.smartcare_failure import SmartcareSCFAPIView
# from .views.smartcare import SmartcareIntegrationAPIView
# from .views.splunk import SplunkIntegrationAPIView
# from .views.npp import NppIntegration15minAPIView, NppIntegrationHourlyAPIView
# from .views.viavi import ViaviIntegrationAPIView
# from .views.remedy import RemedyIntegrationMain
# from .views.eniq import EniqIntegrationAPIView
# from .views.smartcare_trino import smartcare_trino
# from .views.abnormal_kpis import SplunkAbnormalAPIView, NppAbnormalAPIView, ViaviAbnormalAPIView
# from .views.root_cause import RootCauseAPIView
from .views.orchestrated_analysis import (
    AnalysisOrchestratorAPIView, RCAStatusAPIView, RCAFilesAPIView,
    RCAFileDownloadAPIView,
)
from .views.stcc_analysis import STCCAnalysisAPIView
from .views.rca_run import RCARunAPIView
from  .views.test_viavi_kpi import ViaviKpiAPIView
from .views.test_remedy_ticket import RemedyKpiAPIView

# from .views.dqi import DQIAPIView
# from .views.trino import trino_test
# from .views.complaint_report import BaseComplainReportAPIView
# from .views.download import DataExportAPIView, ExportFileDownloadAPIView

urlpatterns = [
#     # Smartcare
#     path('smartcare', SmartcareIntegrationAPIView.as_view(), name="smartcare-api"),
#     path('smartcare/<str:job_id>', SmartcareIntegrationAPIView.as_view(), name="smartcare-job-status"),
#     path("smartcare_trino", smartcare_trino.as_view(), name="smartcare-trino"),

#     # Splunk
#     path('splunk', SplunkIntegrationAPIView.as_view(), name="splunk-api"),
#     path('splunk/<str:job_id>', SplunkIntegrationAPIView.as_view(), name="splunk-job-status"),

#     # NPP
#     path('npp/minutes', NppIntegration15minAPIView.as_view(), name="npp-api"),
#     path('npp/minutes/<str:job_id>', NppIntegration15minAPIView.as_view(), name="npp-job-status"),
#     path('npp/hourly', NppIntegrationHourlyAPIView.as_view(), name="npp-hourly-api"),
#     path('npp/hourly/<str:job_id>', NppIntegrationHourlyAPIView.as_view(), name="npp-hourly-job-status"),

#     # Viavi
#     path('viavi', ViaviIntegrationAPIView.as_view(), name="viavi-api"),
#     path('viavi/<str:job_id>', ViaviIntegrationAPIView.as_view(), name="viavi-job-status"),

#     # Remedy
#     path('remedy', RemedyIntegrationMain.as_view(), name="remedy-api"),
#     path('remedy/<str:job_id>', RemedyIntegrationMain.as_view(), name="remedy-job-status"),

#     # Smartcare failure
#     path('smartcare-failure', SmartcareSCFAPIView.as_view(), name='smartcare-failure'),
#     path('smartcare-failure/<str:job_id>', SmartcareSCFAPIView.as_view(), name='smartcare-failure-job-status'),

#     # Abnormal KPIs (the parallelized-Splunk-node fix lives here)
#     path('abnormal/splunk/', SplunkAbnormalAPIView.as_view(), name='abnormal-splunk'),
#     path('abnormal/splunk/<str:job_id>', SplunkAbnormalAPIView.as_view(), name='abnormal-splunk-job-status'),
#     path('abnormal/npp/', NppAbnormalAPIView.as_view(), name='abnormal-npp'),
#     path('abnormal/npp/<str:job_id>', NppAbnormalAPIView.as_view(), name='abnormal-npp-job-status'),
#     path('abnormal/viavi/', ViaviAbnormalAPIView.as_view(), name='abnormal-viavi'),
#     path('abnormal/viavi/<str:job_id>', ViaviAbnormalAPIView.as_view(), name='abnormal-viavi-job-status'),

#     # Root cause (now takes cache_key in POST body instead of reading a global file)
#     path("root-cause", RootCauseAPIView.as_view(), name="root-cause-file"),
#     path("root-cause/<str:filename>", RootCauseAPIView.as_view()),

    # Orchestrated analysis (data-availability driven; retrieval no longer waits
    # on Smartcare -- see orchestration/ANALYSIS.md). Additive; other endpoints
    # are unchanged. request_id == job_id.
    path("analysis", AnalysisOrchestratorAPIView.as_view(), name="orchestrated-analysis"),
    # RCA readiness / serving must be registered BEFORE the generic job-status
    # route so the multi-segment paths are matched first.
    path("analysis/<str:request_id>/rca-status", RCAStatusAPIView.as_view(),
         name="orchestrated-rca-status"),
    path("analysis/<str:request_id>/rca-files", RCAFilesAPIView.as_view(),
         name="orchestrated-rca-files"),
    path("analysis/<str:request_id>/file/<str:filename>", RCAFileDownloadAPIView.as_view(),
         name="orchestrated-rca-file"),
    path("analysis/<str:job_id>", AnalysisOrchestratorAPIView.as_view(),
         name="orchestrated-analysis-job-status"),
    # All systems + the existing RCA HTML tool run headless; returns the tool's results.
    path("rca/run", RCARunAPIView.as_view(), name="rca-run"),
    path("rca/run/<str:job_id>", RCARunAPIView.as_view(), name="rca-run-job-status"),
        # STCC: NPP / Viavi / Remedy only, synchronous (result in the POST response)
    path("stcc/analysis", STCCAnalysisAPIView.as_view(), name="stcc-analysis"),
    path("stcc/analysis/<str:job_id>", STCCAnalysisAPIView.as_view(), name="stcc-analysis-job-status"),
     path(
     "smartcare/site-kpis",SmartcareSiteKpiAPIView.as_view(),name="smartcare-site-kpis",
     ),
     path("npp/kpis", NppKpiAPIView.as_view(), name="npp-kpis"),

     path("viavi/kpis/", ViaviKpiAPIView.as_view(), name="viavi-kpis"),

     path("remedy/kpis/", RemedyKpiAPIView.as_view(), name="remedy-kpis"),
#     # Unchanged / not yet converted (no reported slowness here)
#     path("abnormal-kpis", SplunkAbnormalAPIView.as_view(), name="abnormal-kpi"),  # legacy alias, review if still used
#     path('dqi', DQIAPIView.as_view(), name='dqi'),
#     path('complaint-report', BaseComplainReportAPIView.as_view(), name='complaint-report'),
#     path('export-data', DataExportAPIView.as_view(), name='export-data'),
#     path('export-data/<str:job_id>', DataExportAPIView.as_view(), name='export-data-status'),
#     path('export-data/file/<str:filename>', ExportFileDownloadAPIView.as_view(), name='export-data-file'),

#     path('Eniq', EniqIntegrationAPIView.as_view(), name="eniq-api"),

#     path("trino-test", trino_test.as_view(), name="trino-test"),

]

"""
Frontend polling pattern, same for every system above:
    1. POST <endpoint>            -> { job_id, status: "PENDING" }
    2. GET  <endpoint>/<job_id>   every 1-2s
         -> { status: "RUNNING", step: "...", progress_percent: N, ... }
         -> { status: "DONE", progress_percent: 100, result: {...} }   <- stop, use result
         -> { status: "FAILED", error: "..." }                          <- stop, show error

For Smartcare specifically, result.cache_key and result.ts should now be
stored client-side and sent as `cache_key`/`ts` in the root-cause POST body
-- this replaces reliance on the old last_smartcare_time_window.json file.
"""