from django.urls import path
from .views import builder

urlpatterns = [
    path('builder/', builder, name='ad_hoc_builder'),
]