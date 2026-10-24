# callflow/urls.py
from django.urls import path, include

urlpatterns = [
    path("api/", include("callflow.core.urls")),
    # path("prototype/api/", include("callflow.prototype.urls")),   # legacy path -> MI (callflow/prototype/urls.py shim)
    path("mi/api/", include("callflow.mi.urls")),
    # path('NCI/', include("NCI.urls")), 
    
]
