from django.urls import path
from callflow.prototype import views

urlpatterns = [
    path("smartcare", views.PrototypeSmartcareIntegrationAPIView.as_view(), name="smartcare-api"),
]