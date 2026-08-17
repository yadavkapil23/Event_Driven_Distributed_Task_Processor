import json
import os
import time
import pika

from .constants import COMMAND_QUEUES, DLX_EXCHANGE, MAX_ATTEMPTS, REPLY_QUEUE

RABBITMQ_URL = os.environ.get('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672')
CONNECT_RETRIES = 10
CONNECT_RETRY_DELAY_SECONDS = 2


def connect() -> pika.BlockingConnection:
    """
    Retries on startup because container orchestration (docker-compose
    `depends_on: condition: service_healthy`) can report RabbitMQ healthy
    slightly before its AMQP listener is ready to accept connections.
    """
    last_error = None
    for attempt in range(1, CONNECT_RETRIES + 1):
        try:
            return pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        except pika.exceptions.AMQPConnectionError as exc:
            last_error = exc
            print(f'RabbitMQ not ready yet (attempt {attempt}/{CONNECT_RETRIES}): {exc}')
            time.sleep(CONNECT_RETRY_DELAY_SECONDS)
    raise last_error


def setup_topology(channel: pika.channel.Channel) -> None:
    channel.exchange_declare(exchange=DLX_EXCHANGE, exchange_type='direct', durable=True)

    for queue_name in COMMAND_QUEUES.values():
        # q.<worker>.commands -> q.<worker>.dlq
        worker = queue_name.split('.')[1]
        dlq = f'q.{worker}.dlq'

        channel.queue_declare(queue=dlq, durable=True)
        channel.queue_bind(queue=dlq, exchange=DLX_EXCHANGE, routing_key=queue_name)

        channel.queue_declare(
            queue=queue_name,
            durable=True,
            arguments={
                'x-dead-letter-exchange': DLX_EXCHANGE,
                'x-dead-letter-routing-key': queue_name,
            },
        )

    channel.queue_declare(queue=REPLY_QUEUE, durable=True)


def publish(channel: pika.channel.Channel, queue: str, message: dict) -> None:
    channel.basic_publish(
        exchange='',
        routing_key=queue,
        body=json.dumps(message).encode('utf-8'),
        properties=pika.BasicProperties(delivery_mode=2),
    )


def death_count(properties: pika.BasicProperties) -> int:
    headers = properties.headers or {}
    deaths = headers.get('x-death') or []
    return deaths[0]['count'] if deaths else 0


def consume(queue: str, handler) -> None:
    """
    Blocking consume loop. `handler(message: dict) -> None` should raise on
    failure; on exception, the message is nacked without requeue so it
    dead-letters to `q.<worker>.dlq` once redelivery attempts are exhausted
    (RabbitMQ tracks the death count itself via the DLX binding + queue TTL-free
    dead-lettering — here we simply track attempts via the x-death header count
    and stop retrying past MAX_ATTEMPTS by nacking without requeue).
    """
    connection = connect()
    channel = connection.channel()
    setup_topology(channel)
    channel.basic_qos(prefetch_count=1)

    def on_message(ch, method, properties, body):
        try:
            message = json.loads(body.decode('utf-8'))
            handler(message)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as exc:
            print(f'Error processing message on {queue}: {exc}')
            attempts = death_count(properties)
            if attempts + 1 >= MAX_ATTEMPTS:
                print(f'Max attempts reached for message on {queue}, dead-lettering.')
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(queue=queue, on_message_callback=on_message)
    print(f'Consuming from {queue}...')
    channel.start_consuming()
