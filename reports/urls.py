from django.urls import path
from . import views

urlpatterns = [
    path('builder/', views.builder, name='builder'),
    path('export/excel/', views.export_excel, name='export_excel'),
    path('export/csv/', views.export_csv, name='export_csv'),
    path('export/json/', views.export_json, name='export_json'),
]
