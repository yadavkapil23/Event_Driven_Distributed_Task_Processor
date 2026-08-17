COMMAND_QUEUES = {
    'inventory': 'q.inventory.commands',
    'payment': 'q.payment.commands',
    'shipping': 'q.shipping.commands',
}

REPLY_QUEUE = 'q.orchestrator.replies'
DLX_EXCHANGE = 'dlx.failed'
MAX_ATTEMPTS = 3


def queue_for_step(step: str) -> str:
    if step.startswith('inventory.'):
        return COMMAND_QUEUES['inventory']
    if step.startswith('payment.'):
        return COMMAND_QUEUES['payment']
    return COMMAND_QUEUES['shipping']
