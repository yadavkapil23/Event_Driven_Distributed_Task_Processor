import os
import time

import requests

BASE_URL = os.environ.get('ORCHESTRATOR_URL', 'http://localhost:3000')
HEADERS = {'X-API-Key': os.environ.get('API_KEY', 'dev-local-api-key-change-me')}

ORDERS = [
    {
        'label': 'Happy path order',
        'body': {'customerEmail': 'alice@example.com', 'productId': 1, 'warehouseId': 1, 'quantity': 2},
    },
    {
        'label': 'Forced payment decline (should compensate: release inventory)',
        'body': {
            'customerEmail': 'bob@example.com', 'productId': 2, 'warehouseId': 1,
            'quantity': 1, 'simulateFailure': 'payment',
        },
    },
    {
        'label': 'Forced out-of-stock (should fail before payment/shipping)',
        'body': {
            'customerEmail': 'carol@example.com', 'productId': 3, 'warehouseId': 2,
            'quantity': 1, 'simulateFailure': 'inventory',
        },
    },
    {
        'label': 'Forced carrier timeout (should compensate: refund payment, release inventory)',
        'body': {
            'customerEmail': 'dave@example.com', 'productId': 1, 'warehouseId': 2,
            'quantity': 3, 'simulateFailure': 'shipping',
        },
    },
]


def submit_order(demo):
    print(f"\n--- {demo['label']} ---")
    res = requests.post(f'{BASE_URL}/orders/', json=demo['body'], headers=HEADERS)
    data = res.json()
    print('Response:', data)
    return data


def poll_status(order_id, attempts=10):
    for i in range(attempts):
        time.sleep(1)
        res = requests.get(f'{BASE_URL}/orders/{order_id}/', headers=HEADERS)
        data = res.json()
        saga_status = (data.get('saga') or {}).get('status')
        print(f'  poll {i + 1}: saga status = {saga_status}')
        if saga_status in ('completed', 'failed'):
            return


def main():
    for demo in ORDERS:
        data = submit_order(demo)
        order_id = data.get('orderId')
        if order_id:
            poll_status(order_id)


if __name__ == '__main__':
    main()
