from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

from .consumers import SAGA_PROGRESS_GROUP


def broadcast(saga_id: str, order_id: str, step: str, status: str) -> None:
    channel_layer = get_channel_layer()
    if channel_layer is None:
        return
    async_to_sync(channel_layer.group_send)(
        SAGA_PROGRESS_GROUP,
        {
            'type': 'saga_event',
            'data': {'sagaId': saga_id, 'orderId': order_id, 'step': step, 'status': status},
        },
    )
