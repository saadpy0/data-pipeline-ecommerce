from django.shortcuts import render
from .models import Aggregate, EventCount, TopUser

def dashboard(request):
    context = {
        'data': Aggregate.objects.all().order_by('-total_sales'),
        'event_counts': EventCount.objects.all().order_by('product_name', 'event_type'),
        'top_users': TopUser.objects.all().order_by('-total_purchases')[:10]  # Top 10 users
    }
    return render(request, 'analytics/dashboard.html', context)