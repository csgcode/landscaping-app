from .models import Appointment
from django.http import JsonResponse
from django.views.decorators.http import require_GET

import warnings
import logging

logger = logging.getLogger(__name__)


@require_GET
def appointment_list_view(request):
    """
    DEPRECATED: This view is deprecated.
    
    Use the API endpoint `/api/appointments/` with pagination instead.
    
    This view will be removed in a future release.
    """

    warnings.warn(
        "appointment_list_view is deprecated. Use /api/v1/appointments/ with pagination.",
        stacklevel=2
    )
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
        .order_by("-scheduled_date", "-id")[:100]
    )

    logger.debug("Fetched %d appointments for list view", qs.count())

    return JsonResponse({"appointments": list(qs)})
