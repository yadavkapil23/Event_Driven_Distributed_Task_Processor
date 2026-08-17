import os
import sys
from pathlib import Path

ORCHESTRATOR_DIR = Path(__file__).resolve().parent.parent / 'orchestrator'
sys.path.insert(0, str(ORCHESTRATOR_DIR))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import django
django.setup()

from orders.models import Inventory, Product, Warehouse


def seed():
    Inventory.objects.all().delete()
    Product.objects.all().delete()
    Warehouse.objects.all().delete()

    warehouses = [
        Warehouse.objects.create(name='East Coast Fulfillment', carrier='FedEx'),
        Warehouse.objects.create(name='West Coast Fulfillment', carrier='UPS'),
    ]

    products = [
        Product.objects.create(sku='SKU-001', name='Wireless Mouse', price_cents=2999),
        Product.objects.create(sku='SKU-002', name='Mechanical Keyboard', price_cents=8999),
        Product.objects.create(sku='SKU-003', name='USB-C Hub', price_cents=3499),
    ]

    for warehouse in warehouses:
        for product in products:
            Inventory.objects.create(product=product, warehouse=warehouse, stock=50, reserved=0)

    print(f'Seeded {len(warehouses)} warehouses and {len(products)} products.')


if __name__ == '__main__':
    seed()
