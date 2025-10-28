import django_filters
from rest_framework.exceptions import ValidationError
from .models import Appointment


class AppointmentListFilter(django_filters.FilterSet):
    schedule_start_date = django_filters.IsoDateTimeFilter(
        field_name="scheduled_date", lookup_expr="gte"
    )
    schedule_end_date = django_filters.IsoDateTimeFilter(
        field_name="scheduled_date", lookup_expr="lte"
    )

    status = django_filters.ChoiceFilter(
        choices=Appointment._meta.get_field("status").choices
    )

    def filter_queryset(self, queryset):
        start_date = self.form.cleaned_data.get("schedule_start_date")
        end_date = self.form.cleaned_data.get("schedule_end_date")

        if start_date and end_date and start_date > end_date:
            raise ValidationError(
                {"detail": "schedule_start_date must be <= schedule_end_date"}
            )

        return super().filter_queryset(queryset)

    class Meta:
        model = Appointment
        fields = ["schedule_start_date", "schedule_end_date", "status"]
