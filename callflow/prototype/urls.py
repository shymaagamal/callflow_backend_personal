# Compatibility shim: /callflow/prototype/api/ is served by MI (old contract by default).
from callflow.mi.urls import legacy_urlpatterns as urlpatterns  # noqa: F401
