from django.db import models

# Create your models here.


class Service(models.Model):
    name = models.CharField(max_length=200)
    description = models.TextField()
    base_price = models.DecimalField(max_digits=10, decimal_places=2)
    duration_hours = models.IntegerField()
    is_active = models.BooleanField(default=True)