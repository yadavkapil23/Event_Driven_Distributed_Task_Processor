from .constants import queue_for_step
from .models import Order, SagaState, SagaEvent
from .saga_machine import get_next_action
from .ws import broadcast


def _record_event(saga: SagaState, step: str, event_type: str, payload: dict | None = None) -> None:
    SagaEvent.objects.create(saga=saga, step=step, event_type=event_type, payload=payload)


def _dispatch_step(channel, saga: SagaState, order: Order, step: str, payload: dict) -> None:
    from . import mq

    saga.current_step = step
    saga.save(update_fields=['current_step', 'updated_at'])
    _record_event(saga, step, 'started')
    broadcast(str(saga.saga_id), str(order.id), step, 'started')

    command = {
        'sagaId': str(saga.saga_id),
        'orderId': str(order.id),
        'step': step,
        'idempotencyKey': f'{saga.saga_id}:{step}',
        'attempt': 1,
        'payload': payload,
    }
    mq.publish(channel, queue_for_step(step), command)


def start_saga(channel, order: Order, payload: dict) -> SagaState:
    saga = SagaState.objects.create(order=order, current_step='inventory.reserve', status='running')
    _dispatch_step(channel, saga, order, 'inventory.reserve', payload)
    return saga


def handle_step_result(channel, result: dict, payload: dict) -> None:
    saga_id = result['sagaId']
    order_id = result['orderId']
    step = result['step']
    success = result['success']
    error = result.get('error')

    saga = SagaState.objects.select_related('order').get(saga_id=saga_id)
    order = saga.order

    _record_event(saga, step, 'succeeded' if success else 'failed', {
        'error': error,
        'data': result.get('data'),
    })
    broadcast(saga_id, order_id, step, 'succeeded' if success else 'failed')

    action = get_next_action(step, success)
    action_type = action['type']

    if action_type == 'advance':
        _dispatch_step(channel, saga, order, action['next_step'], payload)

    elif action_type == 'compensate':
        saga.status = 'compensating'
        saga.save(update_fields=['status', 'updated_at'])
        _record_event(saga, action['next_step'], 'compensated', {'triggeredBy': step})
        _dispatch_step(channel, saga, order, action['next_step'], payload)

    elif action_type == 'complete':
        saga.status = 'completed'
        saga.save(update_fields=['status', 'updated_at'])
        order.status = 'completed'
        order.save(update_fields=['status', 'updated_at'])
        broadcast(saga_id, order_id, 'order.completed', 'completed')

    elif action_type == 'fail':
        saga.status = 'failed'
        saga.save(update_fields=['status', 'updated_at'])
        order.status = 'failed'
        order.save(update_fields=['status', 'updated_at'])
        broadcast(saga_id, order_id, 'order.failed', 'failed')
