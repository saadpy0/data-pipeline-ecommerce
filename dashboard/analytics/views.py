from django.shortcuts import render
from .models import Aggregate

def dashboard(request):
    data = Aggregate.objects.all().order_by('-total_sales')
    return render(request, 'analytics/dashboard.html', {'data': data})