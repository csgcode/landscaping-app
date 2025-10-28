from .models import Appointment
from django.http import JsonResponse
from django.views.decorators.http import require_GET

import logging

logger = logging.getLogger(__name__)


@require_GET
def appointment_list_view(request):
    """
    This is a depreciated view, to list the first 100 Appoinments ordered_by scheduled view.
    Use api/ endpoints to get list of appoinments with pagination.
    """
    qs = (
        Appointment.objects.select_related("service", "client")
        .values(
            "id",
            "scheduled_date",
            "status",
            "notes",
            "created_at",
            client_name="client__name",
            service_name="service__name",
        )
        .order_by("-schedled_date", "-id")[:100]
    )

    logger.debug("Fetched %d appointments for list view", qs.count())

    return JsonResponse({"appointments": list(qs)})
