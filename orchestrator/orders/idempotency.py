import json
import os
import redis

IDEMPOTENCY_TTL_SECONDS = 60 * 60 * 24

_client = redis.Redis(
    host=os.environ.get('REDIS_HOST', 'localhost'),
    port=int(os.environ.get('REDIS_PORT', 6379)),
    decode_responses=True,
)


def with_idempotency(idempotency_key: str, fn):
    """
    Ensures `fn` runs at most once per idempotency_key. If the key was already
    processed, returns the cached result instead of re-running `fn` — this is
    what makes redelivered saga step messages safe under at-least-once delivery.

    Only successful results are cached — a raised exception (e.g. a simulated
    decline) must stay retryable, not get permanently memoized as "handled".
    """
    key = f'idem:{idempotency_key}'
    cached = _client.get(key)
    if cached is not None:
        return json.loads(cached)

    result = fn()
    _client.set(key, json.dumps(result), ex=IDEMPOTENCY_TTL_SECONDS)
    return result
