# E-Commerce Order Fulfillment & Inventory Orchestrator

An event-driven, saga-orchestrated order processing system built in Python.
Demonstrates the core hard problems of distributed systems: decoupling
services via a message broker, coordinating a multi-step business
transaction with compensations, guaranteeing idempotent processing under
at-least-once delivery, and maintaining an auditable event trail.

## The business flow

```
order placed -> inventory reserved -> payment charged -> shipping label generated -> customer notified
```

If any step fails, the saga runs compensations in reverse:

```
shipping.cancel -> payment.refund -> inventory.release -> order marked failed
```

## Architecture

```
                 ┌─────────────────────┐
   HTTP POST     │                     │   command queues    ┌──────────────────┐
  /orders/  ───► │  Django + DRF API   │ ───────────────────► │ inventory_worker  │
                 │ (saga orchestrator) │                      └──────────────────┘
                 │                     │ ───────────────────► ┌──────────────────┐
                 │  Postgres:          │                      │  payment_worker   │
                 │   Order             │                      └──────────────────┘
                 │   SagaState         │ ───────────────────► ┌──────────────────┐
                 │   SagaEvent         │                      │ shipping_worker   │
                 │                     │                      └──────────────────┘
                 │  Redis: idempotency │          ◄─────────────────┘  │  │
                 │                     │   q.orchestrator.replies ─────┘
                 └─────────┬───────────┘   (consumed by `consume_replies`
                           │ Django Channels    management command)
                           ▼
                     WebSocket clients
```

Each worker also listens on its own queue for its compensation command
(`inventory.release`, `payment.refund`, `shipping.cancel`), so the same
process handles both the forward and backward paths.

## Why these design choices

**Orchestration over choreography.** A single `SagaState` row and an explicit
state-transition table ([`orchestrator/orders/saga_machine.py`](orchestrator/orders/saga_machine.py))
own the workflow. This keeps the "what happens next" logic in one place,
testable in isolation, instead of scattered across every service's event
handlers.

**Idempotency via Redis.** RabbitMQ gives at-least-once delivery, meaning any
message can be redelivered (worker crash after processing but before ack,
network blip, etc). Every step command carries a deterministic
`idempotencyKey` (`{sagaId}:{step}`); workers wrap their side effects in
[`with_idempotency`](orchestrator/orders/idempotency.py), so a redelivered
message returns the cached result instead of double-charging a card or
double-decrementing stock. Only successful results are cached — a simulated
failure must stay retryable.

**Dead-letter queues.** Each command queue is configured with a dead-letter
exchange (`dlx.failed`). A message that a worker can't process after
`MAX_ATTEMPTS` retries lands in that worker's `.dlq` queue instead of being
silently dropped or looping forever — visible and inspectable via the
RabbitMQ management UI at `localhost:15672`.

**Append-only audit trail.** Every state transition writes a row to
`SagaEvent` (step, event type, payload, timestamp) rather than mutating
history. This is the audit-log / event-sourcing story: at any point you can
reconstruct exactly what happened to an order and when, via `GET /orders/<id>/`.

**Raw pika over Celery.** RabbitMQ topology (queues, exchanges, DLQs, retry
counting via the `x-death` header) is hand-rolled in
[`orchestrator/orders/mq.py`](orchestrator/orders/mq.py) rather than hidden
behind a task-queue framework, so the reliability mechanics are visible and
explainable rather than "it just works because Celery does it."

## Stack

Python, Django + Django REST Framework (API), Django Channels + `channels-redis`
(WebSocket push), `pika` (RabbitMQ client), PostgreSQL (via Django ORM), Redis
(idempotency + Channels layer).

## Project layout

```
orchestrator/            Django project
  config/                 settings, urls, asgi (Channels routing)
  orders/
    models.py              Product, Warehouse, Inventory, Order, SagaState, SagaEvent
    saga_machine.py         pure state-transition function (unit-testable)
    saga_runner.py          dispatches commands, applies transitions, writes audit rows
    mq.py                   pika topology (queues, DLX/DLQs), publish/consume helpers
    idempotency.py          Redis-backed with_idempotency()
    consumers.py / routing.py / ws.py   Channels WebSocket consumer + broadcast helper
    views.py / serializers.py / urls.py  POST /orders/, GET /orders/<id>/
    management/commands/consume_replies.py   pika consumer driving the saga forward
workers/
  inventory_worker.py, payment_worker.py, shipping_worker.py
  _bootstrap.py            bootstraps Django so workers can reuse orders.mq / models
scripts/
  seed.py                  seeds fake products/warehouses/inventory
  demo.py                  fires happy path + 3 forced-failure orders, polls status
```

## Running it

Prerequisites: Docker Desktop, Python 3.10+.

```bash
# 1. Bring up infra (Postgres on host port 5433, to avoid clashing with any
#    native Postgres install already using 5432)
docker compose up -d

# 2. Create a virtualenv and install deps
python -m venv .venv
./.venv/Scripts/pip install -r requirements.txt      # Windows
# source .venv/bin/activate && pip install -r requirements.txt   # macOS/Linux

# 3. Configure env
cp .env.example .env

# 4. Migrate and seed
cd orchestrator
../.venv/Scripts/python manage.py migrate
cd ..
./.venv/Scripts/python scripts/seed.py

# 5. Start all 5 processes, each in its own terminal:
cd orchestrator
../.venv/Scripts/daphne -b 0.0.0.0 -p 3000 config.asgi:application   # API + WebSocket
../.venv/Scripts/python manage.py consume_replies                    # saga reply consumer
cd ..
./.venv/Scripts/python workers/inventory_worker.py
./.venv/Scripts/python workers/payment_worker.py
./.venv/Scripts/python workers/shipping_worker.py

# 6. Fire demo orders (happy path + 3 forced failure modes)
./.venv/Scripts/python scripts/demo.py
```

RabbitMQ management UI: http://localhost:15672 (guest/guest) — inspect
queues, DLQs, and message rates live.

## API

Both endpoints require an `X-API-Key` header matching the `API_KEY` env var,
and are rate-limited to 60 requests/minute per IP.

- `POST /orders/` — `{ customerEmail, productId, warehouseId, quantity, simulateFailure? }`
  `simulateFailure` is one of `"inventory" | "payment" | "shipping"`, for
  demoing the compensation path deterministically. Returns `{ orderId, sagaId }` (202).
- `GET /orders/<id>/` — current order + saga status + full `saga_events` history.
- WebSocket at `ws://localhost:3000/ws/orders/` — broadcasts
  `{ sagaId, orderId, step, status }` on every saga transition.

```bash
curl -X POST http://localhost:3000/orders/ \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d '{"customerEmail":"alice@example.com","productId":1,"warehouseId":1,"quantity":2}'
```

## Failure scenarios the demo exercises

| Scenario | What happens |
|---|---|
| Happy path | All 4 steps succeed, saga marked `completed` |
| Forced payment decline | Inventory reserved, payment fails, saga compensates: releases inventory, saga marked `failed` |
| Forced out-of-stock | Inventory reservation fails immediately, saga marked `failed` (payment/shipping never invoked) |
| Forced carrier timeout | Inventory reserved + payment charged, shipping fails, saga compensates: refunds payment, releases inventory |

## Deploying with Docker

`docker-compose.prod.yml` builds one shared image (from the root `Dockerfile`)
containing both `orchestrator/` and `workers/`, and runs all 5 app processes
as separate containers from that image, differing only in their `command:`
— alongside Postgres, Redis, and RabbitMQ. The existing `docker-compose.yml`
is unchanged and still useful for local dev (infra only, app runs natively).

```bash
# 1. Configure secrets — set real values for DJANGO_SECRET_KEY and API_KEY,
#    not the dev defaults in .env.example
cp .env.example .env

# 2. Build and start everything
docker compose -f docker-compose.prod.yml up -d --build

# 3. Migrate and seed (one-off, against the containerized Postgres)
docker compose -f docker-compose.prod.yml exec web python manage.py migrate
docker compose -f docker-compose.prod.yml exec web python /app/scripts/seed.py

# 4. Hit the API
curl -X POST http://localhost:3000/orders/ \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d '{"customerEmail":"alice@example.com","productId":1,"warehouseId":1,"quantity":2}'
```

**Before exposing this beyond localhost:**
- Set `DJANGO_DEBUG=False` and real (long, random) values for `DJANGO_SECRET_KEY`
  and `API_KEY` — with `DEBUG=False`, Django refuses to start if either is unset.
- Set `DJANGO_ALLOWED_HOSTS` to your actual domain(s).
- Put this behind a reverse proxy (nginx, Caddy, or your cloud provider's load
  balancer) for TLS termination — this project does not terminate HTTPS itself.
- Swap the local Postgres/Redis/RabbitMQ containers for managed equivalents
  (RDS, ElastiCache, CloudAMQP, etc.) if running for real rather than as a demo.

## What's deliberately out of scope

- React/WebSocket dashboard UI — the WebSocket feed exists and is ready to
  consume, but no frontend is built yet.
- Kafka — RabbitMQ was chosen for simpler native DLQ/retry primitives.
- Real payment/carrier integrations — `payment_worker.py` and
  `shipping_worker.py` simulate their steps rather than calling a real
  gateway/carrier API; the saga/idempotency/DLQ mechanics around them are real.
- Managed cloud infra, health-check-based auto-restart, log aggregation,
  and DLQ-depth alerting — natural next steps for a real production deployment,
  not required to demonstrate the distributed-systems patterns this project
  is about.
