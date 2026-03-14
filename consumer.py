import json
import os
import time
from typing import Any
from dotenv import load_dotenv
load_dotenv()

import redis
from elastic_search import es_client, INDEX_NAME
from embeddings import get_embedding  # your embedding function
from text_processor import process_text

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
QUEUE_NAME = os.getenv("REDIS_QUEUE_NAME", "job_queue")
POLL_TIMEOUT_SECONDS = int(os.getenv("REDIS_POLL_TIMEOUT_SECONDS", "5"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "20"))


def get_redis_client() -> redis.Redis:
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)


def parse_job(raw_data: str) -> dict[str, Any] | None:
    try:
        return json.loads(raw_data)
    except json.JSONDecodeError as e:
        print(f"[worker] Failed to decode job JSON: {e}; raw={raw_data!r}")
        return None


def prepare_document(job: dict) -> dict | None:
    """Enrich a raw job dict with its embedding, ready for indexing."""
    try:
        embed_text = process_text(job.get("title"), job.get("excerpt"), job.get("content"))
        embedding = get_embedding(embed_text)
        return {
            "_id": job.get("content_hash"),
            "title": job.get("title"),
            "excerpt": job.get("excerpt"),
            "content": job.get("content"),
            "site_name": job.get("site_name"),
            "language": job.get("language"),
            "byline": job.get("byline"),
            "url": job.get("url"),
            "crawled_at": job.get("crawled_at"),
            "content_hash": job.get("content_hash"),
            "semantic_text_embedding": embedding,
        }
    except Exception as e:
        print(f"[worker] Failed to prepare document '{job.get('url')}': {e}")
        return None


def drain_batch(client: redis.Redis, batch_size: int) -> list[dict]:
    """
    Non-blocking drain — pull up to batch_size items from the queue
    that are already sitting there right now.
    """
    batch = []
    for _ in range(batch_size):
        result = client.lpop(QUEUE_NAME)   # non-blocking
        if result is None:
            break
        job = parse_job(result)
        if job:
            batch.append(job)
    return batch


def process_batch(jobs: list[dict]) -> None:
    docs = [doc for job in jobs if (doc := prepare_document(job)) is not None]
    if docs:
        es_client.bulk_insert(INDEX_NAME, docs, refresh="wait_for")
        print(f"[worker] Bulk indexed {len(docs)} documents.")


def run_worker_loop() -> None:
    client = get_redis_client()
    print(f"[worker] Connected to Redis at {REDIS_URL}, listening on '{QUEUE_NAME}'")

    while True:
        try:
            # Block until at least one item arrives
            result = client.blpop(QUEUE_NAME, timeout=POLL_TIMEOUT_SECONDS)

            if result is None:
                continue  # timeout, nothing in queue

            _queue, raw_data = result
            first_job = parse_job(raw_data)

            # Drain whatever else is already in the queue right now
            rest = drain_batch(client, batch_size=BATCH_SIZE - 1)
            batch = ([first_job] if first_job else []) + rest

            print(f"[worker] Processing batch of {len(batch)} jobs.")
            process_batch(batch)

        except KeyboardInterrupt:
            print("[worker] Shutting down.")
            break
        except Exception as e:
            print(f"[worker] Error: {e}")
            time.sleep(1)


if __name__ == "__main__":
    run_worker_loop()