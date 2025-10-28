import logging

from rest_framework.generics import ListAPIView

from .serializers import AppointmentListSerializer
from .models import Appointment
from .filters import AppointmentListFilter
from .pagination import DefaultLimitOffsetPagination


logger = logging.getLogger(__name__)


class AppointmentListView(ListAPIView):

    serializer_class = AppointmentListSerializer
    pagination_class = DefaultLimitOffsetPagination
    filterset_class = AppointmentListFilter
    queryset = Appointment.objects.select_related("client", "service").order_by(
        "-scheduled_date", "-id"
    )
