from django.urls import path
from . import views

from .views import grafico_dinamico_relatorio

urlpatterns = [
    path('builder/', views.builder, name='builder'),
    path('export/<str:format>/', views.export_data, name='export_data'),

    path('grafico_dinamico_relatorio/', grafico_dinamico_relatorio, name='grafico_dinamico_relatorio'),
]


