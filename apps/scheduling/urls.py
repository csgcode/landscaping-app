from django.urls import path
from .api_views import AppointmentListView

app_name = "scheduling"
urlpatterns = [
    path("appointments/", AppointmentListView.as_view(), name="appointments-list")
]
