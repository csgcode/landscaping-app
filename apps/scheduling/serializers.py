from rest_framework import serializers
from .models import Appointment


class AppointmentListSerializer(serializers.ModelSerializer):
    client_name = serializers.CharField(source="client.name", read_only=True)
    service_name = serializers.CharField(source="service.name", read_only=True)

    class Meta:
        model = Appointment
        fields = (
            "id",
            "scheduled_date",
            "status",
            "notes",
            "created_at",
            "client_name",
            "service_name",
        )
