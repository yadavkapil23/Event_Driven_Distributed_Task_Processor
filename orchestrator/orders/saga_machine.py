"""
Explicit saga state machine: given the step that just finished and whether it
succeeded, decide what happens next. Kept as pure data + a pure function so
it's trivially testable in isolation from RabbitMQ/Postgres.

Happy path:   inventory.reserve -> payment.charge -> shipping.label -> complete
Compensation: shipping.cancel -> payment.refund -> inventory.release -> fail
"""

# step -> {"type": "advance", "next_step": ...} | {"type": "complete"}
FORWARD_TRANSITIONS = {
    'inventory.reserve': {'type': 'advance', 'next_step': 'payment.charge'},
    'payment.charge': {'type': 'advance', 'next_step': 'shipping.label'},
    'shipping.label': {'type': 'complete'},
}

# On failure of a forward step, which compensation step do we enter first?
COMPENSATION_ENTRY = {
    'shipping.label': 'shipping.cancel',
    'payment.charge': 'payment.refund',
    'inventory.reserve': 'inventory.release',
}

# After a compensation step succeeds, which compensation step runs next?
COMPENSATION_CHAIN = {
    'shipping.cancel': 'payment.refund',
    'payment.refund': 'inventory.release',
}

COMPENSATION_STEPS = {'shipping.cancel', 'payment.refund', 'inventory.release'}


def get_next_action(step: str, success: bool) -> dict:
    if step in COMPENSATION_STEPS:
        # Whether it succeeds or fails, a compensation step just moves to the
        # next compensation in the chain (or terminates the saga as failed) —
        # we don't retry-cascade compensation failures into further compensation.
        next_step = COMPENSATION_CHAIN.get(step)
        return {'type': 'compensate', 'next_step': next_step} if next_step else {'type': 'fail'}

    if success:
        return FORWARD_TRANSITIONS.get(step, {'type': 'fail'})

    entry = COMPENSATION_ENTRY.get(step)
    return {'type': 'compensate', 'next_step': entry} if entry else {'type': 'fail'}
