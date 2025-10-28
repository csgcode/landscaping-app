from django.db import models
from django.contrib.auth.models import User
from apps.services.models import Service

class Client(models.Model):
    name = models.CharField(max_length=200)
    email = models.EmailField()
    phone = models.CharField(max_length=20)
    address = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)


class TeamMember(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    specialties = models.ManyToManyField(Service)
    is_available = models.BooleanField(default=True)