FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY orchestrator/ orchestrator/
COPY workers/ workers/
COPY scripts/ scripts/

WORKDIR /app/orchestrator

# No default CMD — docker-compose.prod.yml sets `command:` per service
# (daphne for the API, `manage.py consume_replies` for the reply consumer,
# or a workers/*.py script), all from this one shared image.
