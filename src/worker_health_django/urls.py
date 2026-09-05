"""URLs for the optional in-app health views.

    urlpatterns = [
        path("internal/health/", include("worker_health_django.urls")),
    ]

Mounted under a prefix on purpose: these are internal endpoints, and a
prefix is what lets an ingress or a network policy exclude them in one rule
rather than five. See docs/OPERATIONS.md before exposing them anywhere.
"""
from __future__ import annotations

from django.urls import path

from . import views

app_name = "worker_health"

urlpatterns = [
    path("live", views.live, name="live"),
    path("ready", views.ready, name="ready"),
    path("health", views.health, name="health"),
    path("config", views.config, name="config"),
    path("events", views.events, name="events"),
    path("", views.health, name="index"),
]
