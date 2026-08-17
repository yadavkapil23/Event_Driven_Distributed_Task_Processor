import _bootstrap  # noqa: F401 — must run before importing orders.*

from orders import mq
from orders.constants import COMMAND_QUEUES, REPLY_QUEUE
from orders.idempotency import with_idempotency


def charge(command: dict) -> dict:
    saga_id, order_id, payload = command['sagaId'], command['orderId'], command['payload']

    if payload.get('simulateFailure') == 'payment':
        return {
            'sagaId': saga_id, 'orderId': order_id, 'step': 'payment.charge',
            'success': False, 'error': 'Card declined (simulated)',
        }

    def do_charge():
        # Simulated payment gateway call.
        return {'transactionId': f'txn_{saga_id}', 'amountCents': payload['totalCents']}

    data = with_idempotency(command['idempotencyKey'], do_charge)
    return {'sagaId': saga_id, 'orderId': order_id, 'step': 'payment.charge', 'success': True, 'data': data}


def refund(command: dict) -> dict:
    saga_id, order_id, payload = command['sagaId'], command['orderId'], command['payload']

    def do_refund():
        return {'refundedCents': payload['totalCents']}

    data = with_idempotency(command['idempotencyKey'], do_refund)
    return {'sagaId': saga_id, 'orderId': order_id, 'step': 'payment.refund', 'success': True, 'data': data}


def main():
    publish_connection = mq.connect()
    publish_channel = publish_connection.channel()
    mq.setup_topology(publish_channel)

    def handler(command: dict) -> None:
        print(f"[payment-worker] handling {command['step']} for saga {command['sagaId']}")
        result = charge(command) if command['step'] == 'payment.charge' else refund(command)
        mq.publish(publish_channel, REPLY_QUEUE, result)

    print('payment-worker ready.')
    mq.consume(COMMAND_QUEUES['payment'], handler)


if __name__ == '__main__':
    main()
