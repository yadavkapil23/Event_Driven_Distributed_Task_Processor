import _bootstrap  # noqa: F401 — must run before importing orders.*

from django.db import transaction

from orders import mq
from orders.constants import COMMAND_QUEUES, REPLY_QUEUE
from orders.idempotency import with_idempotency
from orders.models import Inventory


def reserve(command: dict) -> dict:
    saga_id, order_id, payload = command['sagaId'], command['orderId'], command['payload']

    if payload.get('simulateFailure') == 'inventory':
        return {
            'sagaId': saga_id, 'orderId': order_id, 'step': 'inventory.reserve',
            'success': False, 'error': 'Out of stock (simulated)',
        }

    try:
        def do_reserve():
            with transaction.atomic():
                inv = Inventory.objects.select_for_update().get(
                    product_id=payload['productId'], warehouse_id=payload['warehouseId']
                )
                if inv.stock - inv.reserved < payload['quantity']:
                    raise ValueError('Insufficient stock')
                inv.reserved += payload['quantity']
                inv.save(update_fields=['reserved'])
            return {'reserved': payload['quantity']}

        with_idempotency(command['idempotencyKey'], do_reserve)
        return {'sagaId': saga_id, 'orderId': order_id, 'step': 'inventory.reserve', 'success': True}
    except Exception as exc:
        return {
            'sagaId': saga_id, 'orderId': order_id, 'step': 'inventory.reserve',
            'success': False, 'error': str(exc),
        }


def release(command: dict) -> dict:
    saga_id, order_id, payload = command['sagaId'], command['orderId'], command['payload']

    def do_release():
        with transaction.atomic():
            inv = Inventory.objects.select_for_update().get(
                product_id=payload['productId'], warehouse_id=payload['warehouseId']
            )
            inv.reserved = max(inv.reserved - payload['quantity'], 0)
            inv.save(update_fields=['reserved'])
        return {'released': payload['quantity']}

    with_idempotency(command['idempotencyKey'], do_release)
    return {'sagaId': saga_id, 'orderId': order_id, 'step': 'inventory.release', 'success': True}


def main():
    publish_connection = mq.connect()
    publish_channel = publish_connection.channel()
    mq.setup_topology(publish_channel)

    def handler(command: dict) -> None:
        print(f"[inventory-worker] handling {command['step']} for saga {command['sagaId']}")
        result = reserve(command) if command['step'] == 'inventory.reserve' else release(command)
        mq.publish(publish_channel, REPLY_QUEUE, result)

    print('inventory-worker ready.')
    mq.consume(COMMAND_QUEUES['inventory'], handler)


if __name__ == '__main__':
    main()
