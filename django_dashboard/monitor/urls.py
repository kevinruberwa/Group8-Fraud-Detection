from django.urls import path

from . import views

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("api/summary/", views.summary_api, name="summary_api"),
]
