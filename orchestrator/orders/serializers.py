from rest_framework import serializers
from .models import Order, SagaState, SagaEvent


class CreateOrderSerializer(serializers.Serializer):
    customerEmail = serializers.EmailField()
    productId = serializers.IntegerField()
    warehouseId = serializers.IntegerField()
    quantity = serializers.IntegerField(min_value=1)
    simulateFailure = serializers.ChoiceField(
        choices=['inventory', 'payment', 'shipping'], required=False, allow_null=True
    )


class SagaEventSerializer(serializers.ModelSerializer):
    class Meta:
        model = SagaEvent
        fields = ['id', 'step', 'event_type', 'payload', 'created_at']


class SagaStateSerializer(serializers.ModelSerializer):
    class Meta:
        model = SagaState
        fields = ['saga_id', 'current_step', 'status', 'created_at', 'updated_at']


class OrderSerializer(serializers.ModelSerializer):
    class Meta:
        model = Order
        fields = [
            'id', 'customer_email', 'product', 'warehouse', 'quantity',
            'total_cents', 'status', 'simulate_failure', 'created_at', 'updated_at',
        ]
