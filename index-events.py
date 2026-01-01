import json
import logging
import os
import time
from typing import Any, Dict, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams, PointStruct
from sentence_transformers import SentenceTransformer

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_IN = os.getenv("TOPIC_IN", "verified-events")
GROUP_ID = os.getenv("GROUP_ID", "index-events")

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
COLLECTION = os.getenv("QDRANT_COLLECTION", "events")

MODEL_NAME = os.getenv("EMB_MODEL", "all-MiniLM-L6-v2")

# If you want to re-index from the beginning after a reset:
AUTO_OFFSET_RESET = os.getenv(
    "AUTO_OFFSET_RESET", "earliest"
)  # earliest | latest


def safe_get(d: Dict[str, Any], key: str, default=None):
    v = d.get(key, default)
    return default if v is None else v


def make_point_id(event_id: str) -> int:
    # stable-ish 63-bit id
    return abs(hash(event_id)) % (2**63)


def ensure_collection(client: QdrantClient, dim: int):
    existing = [c.name for c in client.get_collections().collections]
    if COLLECTION in existing:
        logging.info("Qdrant collection '%s' already exists.", COLLECTION)
        return

    logging.info("Creating Qdrant collection '%s' (dim=%d)", COLLECTION, dim)
    client.create_collection(
        collection_name=COLLECTION,
        vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
    )


def qdrant_count(client: QdrantClient) -> int:
    try:
        res = client.count(collection_name=COLLECTION, exact=True)
        return int(res.count)
    except Exception as e:
        logging.error("Failed to count points in Qdrant: %s", e)
        return -1


def build_doc(ev: Dict[str, Any]) -> str:
    event_type = safe_get(ev, "event_type", "")
    source = safe_get(ev, "source", "")
    text = safe_get(ev, "text", "")
    place = ""
    raw = ev.get("raw") or {}
    if isinstance(raw, dict):
        place = raw.get("place") or ""
    parts = [
        f"type={event_type}",
        f"source={source}",
    ]
    if place:
        parts.append(f"place={place}")
    if text:
        parts.append(text)
    return " | ".join(parts).strip()


def main():
    logging.info("Loading embedding model: %s", MODEL_NAME)
    model = SentenceTransformer(MODEL_NAME)
    dim = model.get_sentence_embedding_dimension()

    qdrant = QdrantClient(url=QDRANT_URL)
    ensure_collection(qdrant, dim)

    # IMPORTANT: auto_commit False so you can re-run safely and only commit after successful upsert
    consumer = KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset=AUTO_OFFSET_RESET,
        consumer_timeout_ms=0,  # block forever
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    logging.info(
        "Indexing: Kafka[%s] topic=%s group_id=%s -> Qdrant[%s] collection=%s (offset_reset=%s)",
        KAFKA_BOOTSTRAP,
        TOPIC_IN,
        GROUP_ID,
        QDRANT_URL,
        COLLECTION,
        AUTO_OFFSET_RESET,
    )

    processed = 0
    written = 0
    last_log = time.time()

    while True:
        try:
            msg_pack = consumer.poll(timeout_ms=1000, max_records=50)
        except KafkaError as e:
            logging.error("Kafka poll error: %s", e)
            time.sleep(1)
            continue

        any_msg = False

        for tp, msgs in msg_pack.items():
            for msg in msgs:
                any_msg = True
                processed += 1
                ev = msg.value

                event_id = safe_get(ev, "event_id", "")
                if not event_id:
                    logging.warning("Skipping message with missing event_id")
                    continue

                # Build text to embed
                doc = build_doc(ev)
                if not doc:
                    logging.warning(
                        "Skipping %s because doc is empty", event_id
                    )
                    continue

                vector = model.encode(doc).tolist()
                point_id = make_point_id(event_id)

                payload = {
                    "event_id": event_id,
                    "timestamp_utc_ms": safe_get(ev, "timestamp_utc_ms"),
                    "source": safe_get(ev, "source"),
                    "event_type": safe_get(ev, "event_type"),
                    "url": safe_get(ev, "url"),
                    "tags": safe_get(ev, "tags", []),
                    "geo": safe_get(ev, "geo"),
                    "raw": safe_get(ev, "raw"),
                    "credibility_score": safe_get(ev, "credibility_score"),
                    "verified": safe_get(ev, "verified"),
                    "processed_at_utc_ms": safe_get(ev, "processed_at_utc_ms"),
                    # store BOTH so ask.py can print them
                    "text": safe_get(ev, "text", ""),
                    "place": (
                        (ev.get("raw") or {}).get("place")
                        if isinstance(ev.get("raw"), dict)
                        else None
                    ),
                    "doc": doc,
                }

                # ---- upsert and verify ----
                try:
                    qdrant.upsert(
                        collection_name=COLLECTION,
                        points=[
                            PointStruct(
                                id=point_id, vector=vector, payload=payload
                            )
                        ],
                        wait=True,
                    )
                    written += 1
                    consumer.commit()  # commit only after successful write
                except Exception as e:
                    logging.exception(
                        "Qdrant upsert failed for %s: %s", event_id, e
                    )
                    # do NOT commit offset if write failed
                    continue

                # periodic visibility
                if written % 10 == 0:
                    c = qdrant_count(qdrant)
                    logging.info(
                        "Written=%d (processed=%d). Qdrant count=%d. latest=%s",
                        written,
                        processed,
                        c,
                        event_id,
                    )

        # If nothing arrives, still log once in a while so you know it's alive
        if (time.time() - last_log) > 10:
            last_log = time.time()
            if not any_msg:
                logging.info(
                    "No Kafka messages received in last 10s. (topic=%s, group=%s)",
                    TOPIC_IN,
                    GROUP_ID,
                )


if __name__ == "__main__":
    main()
