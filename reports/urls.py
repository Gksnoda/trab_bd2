from django.urls import path
from . import views

urlpatterns = [
    path('builder/', views.builder, name='builder'),
    path('export/<str:format>/', views.export_data, name='export_data'),
    path('reports/chart/', views.top_streamers_chart, name='top_streamers_chart'),
    path('reports/top_games_chart/', views.top_games_chart, name='top_games_chart'),

]
