from django.db import models


class Service(models.Model):
    class Priority(models.TextChoices):
        LOW = "LOW", "Low"
        MEDIUM = "MEDIUM", "Medium"
        HIGH = "HIGH", "High"

    name = models.CharField(max_length=200)
    description = models.TextField()
    priority = models.CharField(
        max_length=10,
        choices=Priority.choices,
        null = True
    )
    base_price = models.DecimalField(max_digits=10, decimal_places=2)
    duration_hours = models.IntegerField()
    is_active = models.BooleanField(default=True)