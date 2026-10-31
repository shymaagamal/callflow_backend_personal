# callflow/core/urls_patch.py
"""
Full url patterns for the job-backed pattern, covering every system.
Replace the relevant lines in your existing urls.py with these.

Every POST endpoint now returns {job_id, status: "PENDING"} immediately.
Every system gets a matching GET <job_id> route for polling.
"""

from django.urls import path

from callflow.core.views.smartcare_API import SmartcareAPIView
from callflow.core.views.trino_site_kpi import TrinoSiteKpiAPIView


from .views.test_npp_kpi import NppKpiAPIView

from .views.trino_site_kpi import TrinoSiteKpiAPIView
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
from .views.rca_run import RCARunAPIView, RCASiteRunAPIView, RCARunFindingsCSVView, RCARunDiagnosticsView
from  .views.test_viavi_kpi import ViaviKpiAPIView
from .views.test_remedy_ticket import RemedyKpiAPIView
from .views.Voice_KPI_Eniq import SybaseQueryView

# from .views.dqi import DQIAPIView
# from .views.trino import trino_test
# from .views.complaint_report import BaseComplainReportAPIView
# from .views.download import DataExportAPIView, ExportFileDownloadAPIView

urlpatterns =[
    # are unchanged. request_id == job_id.
    path("smartcare" , SmartcareAPIView.as_view(), name="smartcare"),
    path("voice-kpi-eniq", SybaseQueryView.as_view(), name="voice-kpi-eniq"), 
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
    path("rca/run/<str:job_id>/findings.csv", RCARunFindingsCSVView.as_view(), name="rca-run-findings-csv"),
    path("rca/run/<str:job_id>/diagnostics", RCARunDiagnosticsView.as_view(), name="rca-run-diagnostics"),
    # Same, for a site list instead of MSISDNs (no Trino; sites go straight to NPP/Viavi/Remedy).
    path("rca/sites/run", RCASiteRunAPIView.as_view(), name="rca-site-run"),
    path("rca/sites/run/<str:job_id>", RCASiteRunAPIView.as_view(), name="rca-site-run-job-status"),
    path("rca/sites/run/<str:job_id>/findings.csv", RCARunFindingsCSVView.as_view(),
         name="rca-site-run-findings-csv"),
    path("rca/sites/run/<str:job_id>/diagnostics", RCARunDiagnosticsView.as_view(),
         name="rca-site-run-diagnostics"),
        # STCC: NPP / Viavi / Remedy only, synchronous (result in the POST response)
    path("stcc/analysis", STCCAnalysisAPIView.as_view(), name="stcc-analysis"),
    path("stcc/analysis/<str:job_id>", STCCAnalysisAPIView.as_view(), name="stcc-analysis-job-status"),
     path(
     "trino/site-kpis",TrinoSiteKpiAPIView.as_view(),name="trino-site-kpis",
     ),
     path("npp/kpis", NppKpiAPIView.as_view(), name="npp-kpis"),

     path("viavi/kpis/", ViaviKpiAPIView.as_view(), name="viavi-kpis"),
     path("remedy/kpis/", RemedyKpiAPIView.as_view(), name="remedy-kpis"),
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