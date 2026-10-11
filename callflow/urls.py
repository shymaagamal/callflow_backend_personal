# callflow/urls.py
from django.urls import path, include

urlpatterns = [
    path("api/", include("callflow.core.urls")),
    path("prototype/api/", include("callflow.prototype.urls")),
    # path('NCI/', include("NCI.urls")), 
    
]
