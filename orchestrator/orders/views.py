from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response
from rest_framework import status as http_status

from . import mq, saga_runner
from .models import Order, Product, SagaState
from .permissions import HasAPIKey
from .serializers import CreateOrderSerializer, OrderSerializer, SagaStateSerializer, SagaEventSerializer


@api_view(['POST'])
@permission_classes([HasAPIKey])
def create_order(request):
    serializer = CreateOrderSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)
    data = serializer.validated_data

    try:
        product = Product.objects.get(id=data['productId'])
    except Product.DoesNotExist:
        return Response({'error': 'product not found'}, status=http_status.HTTP_404_NOT_FOUND)

    total_cents = product.price_cents * data['quantity']

    order = Order.objects.create(
        customer_email=data['customerEmail'],
        product_id=data['productId'],
        warehouse_id=data['warehouseId'],
        quantity=data['quantity'],
        total_cents=total_cents,
        simulate_failure=data.get('simulateFailure'),
    )

    payload = {
        'customerEmail': order.customer_email,
        'productId': order.product_id,
        'warehouseId': order.warehouse_id,
        'quantity': order.quantity,
        'totalCents': order.total_cents,
        'simulateFailure': order.simulate_failure,
    }

    connection = mq.connect()
    channel = connection.channel()
    mq.setup_topology(channel)
    try:
        saga = saga_runner.start_saga(channel, order, payload)
    finally:
        connection.close()

    return Response(
        {'orderId': str(order.id), 'sagaId': str(saga.saga_id)},
        status=http_status.HTTP_202_ACCEPTED,
    )


@api_view(['GET'])
def get_order(request, order_id):
    try:
        order = Order.objects.get(id=order_id)
    except Order.DoesNotExist:
        return Response({'error': 'order not found'}, status=http_status.HTTP_404_NOT_FOUND)

    saga = SagaState.objects.filter(order=order).first()
    events = saga.events.all() if saga else []

    return Response({
        'order': OrderSerializer(order).data,
        'saga': SagaStateSerializer(saga).data if saga else None,
        'events': SagaEventSerializer(events, many=True).data,
    })
