"""
Per-system analyzers.

Each analyzer answers exactly one question: "everything required to analyze
<system>." It DELEGATES the actual business math to the existing, unchanged
services (``kpi_analyzer`` thresholds, ``npp_service`` enrichment,
``smartcare_service`` metrics/remedy) and uses the pure ``dashboard_shaping``
helpers to build the frontend-ready arrays. The DAG (pipeline.py) only wires
dependencies; the analyzers hold the meaning.

Every analyzer returns the same result contract (see ``base.build_result``):

    { status, system, summary, metrics, affected, details, abnormal_kpis,
      insights, error }

Business functions are injectable so the analyzers are unit-testable without
pandas/Django/JVM; defaults lazily import the real implementations.
"""

from .base import build_result, failed_result, SUCCESS, FAILED
from .npp_analyzer import NppAnalyzer
from .splunk_analyzer import SplunkAnalyzer
from .viavi_analyzer import ViaviAnalyzer
from .remedy_analyzer import RemedyAnalyzer
from .smartcare_analyzer import SmartcareAnalyzer

__all__ = [
    "build_result", "failed_result", "SUCCESS", "FAILED",
    "NppAnalyzer", "SplunkAnalyzer", "ViaviAnalyzer",
    "RemedyAnalyzer", "SmartcareAnalyzer",
]
