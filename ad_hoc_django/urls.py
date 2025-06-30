from django.contrib import admin
from django.urls import path, include
from django.shortcuts import redirect

urlpatterns = [
    path('', lambda request: redirect('/reports/builder/')),
    path('admin/', admin.site.urls),
    path('reports/', include('reports.urls')),
]
