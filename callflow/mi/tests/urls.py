# URLconf for the MI tests: only the two MI mounts (the real callflow/urls.py also loads core).
from django.urls import include, path

urlpatterns = [
    path("callflow/prototype/api/", include("callflow.prototype.urls")),
    path("callflow/mi/api/", include("callflow.mi.urls")),
]
