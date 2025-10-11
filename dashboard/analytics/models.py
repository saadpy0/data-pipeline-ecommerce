from django.db import models

class Aggregate(models.Model):
    product_id = models.CharField(max_length=50, unique=True)
    product_name = models.CharField(max_length=100)
    total_sales = models.IntegerField()
    total_revenue = models.FloatField()

    class Meta:
        db_table = "aggregates"

    def __str__(self):
        return f"{self.product_name} ({self.total_sales})"

class EventCount(models.Model):
    product_id = models.CharField(max_length=50)
    product_name = models.CharField(max_length=100)
    event_type = models.CharField(max_length=50)
    count = models.IntegerField()

    class Meta:
        db_table = "event_counts"
        unique_together = ['product_id', 'event_type']

    def __str__(self):
        return f"{self.product_name} - {self.event_type}: {self.count}"

class TopUser(models.Model):
    user_id = models.CharField(max_length=50, unique=True)
    total_purchases = models.IntegerField()
    total_spent = models.FloatField()
    last_purchase_time = models.DateTimeField()

    class Meta:
        db_table = "top_users"

    def __str__(self):
        return f"{self.user_id} ({self.total_purchases} purchases)"