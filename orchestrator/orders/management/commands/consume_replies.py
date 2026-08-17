from django.core.management.base import BaseCommand

from orders import mq, saga_runner
from orders.constants import REPLY_QUEUE
from orders.models import Order


class Command(BaseCommand):
    help = 'Consume saga step results from the orchestrator reply queue and drive the saga forward.'

    def handle(self, *args, **options):
        publish_connection = mq.connect()
        publish_channel = publish_connection.channel()
        mq.setup_topology(publish_channel)

        def handler(result: dict) -> None:
            order = Order.objects.get(id=result['orderId'])
            payload = {
                'customerEmail': order.customer_email,
                'productId': order.product_id,
                'warehouseId': order.warehouse_id,
                'quantity': order.quantity,
                'totalCents': order.total_cents,
                'simulateFailure': order.simulate_failure,
            }
            saga_runner.handle_step_result(publish_channel, result, payload)

        self.stdout.write(self.style.SUCCESS('Reply consumer ready.'))
        mq.consume(REPLY_QUEUE, handler)
