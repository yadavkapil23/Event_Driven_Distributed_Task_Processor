"""
Adds the Django project to sys.path and initializes Django, so worker scripts
can reuse orders.mq / orders.idempotency / orders.models directly instead of
duplicating RabbitMQ, Redis, and ORM plumbing.
"""
import os
import sys
from pathlib import Path

ORCHESTRATOR_DIR = Path(__file__).resolve().parent.parent / 'orchestrator'
sys.path.insert(0, str(ORCHESTRATOR_DIR))

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

import django
django.setup()
