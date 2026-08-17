import json
from channels.generic.websocket import AsyncWebsocketConsumer

SAGA_PROGRESS_GROUP = 'saga_progress'


class SagaProgressConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.channel_layer.group_add(SAGA_PROGRESS_GROUP, self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(SAGA_PROGRESS_GROUP, self.channel_name)

    async def saga_event(self, event):
        await self.send(text_data=json.dumps(event['data']))
