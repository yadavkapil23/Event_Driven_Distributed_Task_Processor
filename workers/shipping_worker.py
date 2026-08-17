import time

import _bootstrap  # noqa: F401 — must run before importing orders.*

from orders import mq
from orders.constants import COMMAND_QUEUES, REPLY_QUEUE
from orders.idempotency import with_idempotency


def label(command: dict) -> dict:
    saga_id, order_id, payload = command['sagaId'], command['orderId'], command['payload']

    if payload.get('simulateFailure') == 'shipping':
        time.sleep(0.5)  # simulate a carrier timeout before failing
        return {
            'sagaId': saga_id, 'orderId': order_id, 'step': 'shipping.label',
            'success': False, 'error': 'Carrier timeout (simulated)',
        }

    def do_label():
        return {'trackingNumber': f'TRACK-{saga_id[:8].upper()}'}

    data = with_idempotency(command['idempotencyKey'], do_label)
    return {'sagaId': saga_id, 'orderId': order_id, 'step': 'shipping.label', 'success': True, 'data': data}


def cancel(command: dict) -> dict:
    saga_id, order_id = command['sagaId'], command['orderId']

    def do_cancel():
        return {'cancelled': True}

    with_idempotency(command['idempotencyKey'], do_cancel)
    return {'sagaId': saga_id, 'orderId': order_id, 'step': 'shipping.cancel', 'success': True}


def main():
    publish_connection = mq.connect()
    publish_channel = publish_connection.channel()
    mq.setup_topology(publish_channel)

    def handler(command: dict) -> None:
        print(f"[shipping-worker] handling {command['step']} for saga {command['sagaId']}")
        result = label(command) if command['step'] == 'shipping.label' else cancel(command)
        mq.publish(publish_channel, REPLY_QUEUE, result)

    print('shipping-worker ready.')
    mq.consume(COMMAND_QUEUES['shipping'], handler)


if __name__ == '__main__':
    main()
