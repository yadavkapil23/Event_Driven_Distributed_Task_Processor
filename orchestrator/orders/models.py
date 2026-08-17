import uuid
from django.db import models


class Warehouse(models.Model):
    name = models.TextField()
    carrier = models.TextField()

    def __str__(self):
        return self.name


class Product(models.Model):
    sku = models.TextField(unique=True)
    name = models.TextField()
    price_cents = models.IntegerField()

    def __str__(self):
        return self.name


class Inventory(models.Model):
    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    warehouse = models.ForeignKey(Warehouse, on_delete=models.CASCADE)
    stock = models.IntegerField(default=0)
    reserved = models.IntegerField(default=0)

    class Meta:
        unique_together = ('product', 'warehouse')


class Order(models.Model):
    STATUS_CHOICES = [
        ('pending', 'pending'),
        ('completed', 'completed'),
        ('failed', 'failed'),
    ]
    FAILURE_CHOICES = [
        ('inventory', 'inventory'),
        ('payment', 'payment'),
        ('shipping', 'shipping'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    customer_email = models.TextField()
    product = models.ForeignKey(Product, on_delete=models.PROTECT)
    warehouse = models.ForeignKey(Warehouse, on_delete=models.PROTECT)
    quantity = models.IntegerField()
    total_cents = models.IntegerField()
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    simulate_failure = models.CharField(max_length=20, choices=FAILURE_CHOICES, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)


class SagaState(models.Model):
    STATUS_CHOICES = [
        ('running', 'running'),
        ('completed', 'completed'),
        ('compensating', 'compensating'),
        ('failed', 'failed'),
    ]

    saga_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    order = models.ForeignKey(Order, on_delete=models.CASCADE)
    current_step = models.TextField()
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='running')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)


class SagaEvent(models.Model):
    EVENT_TYPE_CHOICES = [
        ('started', 'started'),
        ('succeeded', 'succeeded'),
        ('failed', 'failed'),
        ('compensated', 'compensated'),
    ]

    saga = models.ForeignKey(SagaState, on_delete=models.CASCADE, related_name='events')
    step = models.TextField()
    event_type = models.CharField(max_length=20, choices=EVENT_TYPE_CHOICES)
    payload = models.JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['created_at']
