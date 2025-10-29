import pytest
from datetime import datetime, timedelta
from decimal import Decimal
from rest_framework.test import APIClient

from apps.users.models import Client
from apps.services.models import Service
from apps.scheduling.models import Appointment
from apps.scheduling.api_views import AppointmentListView

@pytest.fixture
def make_appointments():
    c1 = Client.objects.create(
        name="John Doe",
        email="john@example.com",
        phone="+15551234567",
        address="123 Main St",
    )
    c2 = Client.objects.create(
        name="Jane Smith",
        email="jane@example.com",
        phone="+15559876543",
        address="456 Oak Ave",
    )
    s1 = Service.objects.create(
        name="Lawn Mowing",
        description="Mow lawn",
        priority=Service.Priority.MEDIUM,
        base_price=Decimal("45.00"),
        duration_hours=1,
        is_active=True,
    )
    s2 = Service.objects.create(
        name="Hedge Trimming",
        description="Trim hedges",
        priority=Service.Priority.HIGH,
        base_price=Decimal("65.00"),
        duration_hours=2,
        is_active=True,
    )
    base = datetime(2025, 10, 1)
    statuses = ["scheduled", "in_progress", "completed", "cancelled"]
    clients = [c1, c2]
    services = [s1, s2]
    for i in range(35):
        scheduled = base + timedelta(days=i % 30, hours=i % 12)
        Appointment.objects.create(
            client=clients[i % 2],
            service=services[i % 2],
            scheduled_date=scheduled,
            status=statuses[i % 4],
            notes=f"test {i}",
        )
    yield

@pytest.mark.django_db
def test_list_paginates_and_shapes(make_appointments):
    client = APIClient()
    resp = client.get("/appointments/?limit=10&offset=0")
    assert resp.status_code == 200
    data = resp.json()
    assert "results" in data
    results = data["results"]
    assert len(results) == 10
    keys = {"id", "scheduled_date", "status", "client_name", "service_name"}
    assert keys.issubset(results[0].keys())

@pytest.mark.django_db
def test_filters_by_date_and_status(make_appointments):
    client = APIClient()
    url = "/appointments/?from=2025-10-01&to=2025-10-31&status=scheduled"
    resp = client.get(url)
    assert resp.status_code == 200
    results = resp.json()["results"]
    assert results
    for r in results:
        d = r["scheduled_date"][:10]
        assert "2025-10-01" <= d <= "2025-10-31"
        assert r["status"] == "scheduled"

@pytest.mark.django_db
def test_bad_date_returns_400(make_appointments):
    client = APIClient()
    resp = client.get("/appointments/?from=2025-99-01&to=2025-10-31")
    assert resp.status_code == 400

@pytest.mark.django_db
def test_status_validation_returns_400(make_appointments):
    client = APIClient()
    resp = client.get("/appointments/?status=NOPE")
    assert resp.status_code == 400

@pytest.mark.django_db
def test_throttle_hits_429(settings, make_appointments):
    settings.REST_FRAMEWORK = getattr(settings, "REST_FRAMEWORK", {})
    rates = getattr(settings.REST_FRAMEWORK, "DEFAULT_THROTTLE_RATES", {})
    rates["appointments"] = "3/min"
    settings.REST_FRAMEWORK["DEFAULT_THROTTLE_RATES"] = rates
    client = APIClient()
    url = "/appointments/"
    for _ in range(3):
        assert client.get(url).status_code == 200
    resp = client.get(url)
    assert resp.status_code == 429