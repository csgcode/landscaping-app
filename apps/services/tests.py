import pytest
from django.core.exceptions import ValidationError
from apps.services.models import Service


@pytest.mark.django_db
def test_new_service_gets_default_priority():
    s = Service.objects.create(
        name="Hedge Trimming",
        description="Trim hedges",
        base_price=50.00,
        duration_hours=2
    )
    assert s.priority == "MEDIUM"


@pytest.mark.django_db
def test_priority_choices_enforced():
    s = Service.objects.create(
        name="Weeding",
        description="Remove weeds",
        base_price=30.00,
        duration_hours=1
    )
    s.priority = "URGENT"
    with pytest.raises(ValidationError):
        s.full_clean()