from django.urls import path
from .views import AppointmentListView

app_name = "scheduling"
urlpatterns = [
    path("appointments/", AppointmentListView.as_view(), name="appointment-list")
]
