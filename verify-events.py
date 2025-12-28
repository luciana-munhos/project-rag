import json
import logging
import os
import time
from typing import Dict, Optional

from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
IN_TOPIC = os.getenv("IN_TOPIC", "raw-events")
OUT_TOPIC = os.getenv("OUT_TOPIC", "verified-events")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "dlq-events")

DEDUP_TTL_SECS = int(os.getenv("DEDUP_TTL_SECS", "7200"))  # 2h
CLEANUP_EVERY = int(os.getenv("CLEANUP_EVERY", "200"))

TRUSTED_SOURCES = set(os.getenv("TRUSTED_SOURCES", "usgs,eonet").split(","))


def now_utc_ms() -> int:
    return int(time.time() * 1000)


def event_key(ev: dict) -> Optional[str]:
    eid = ev.get("event_id")
    if isinstance(eid, str) and eid.strip():
        return eid.strip()
    src = ev.get("source", "unknown")
    ts = ev.get("timestamp_utc_ms")
    text = ev.get("text", "")
    return f"{src}:{ts}:{hash(text)}" if ts is not None else None


def is_valid_event(ev: dict) -> bool:
    return (
        isinstance(ev, dict)
        and isinstance(ev.get("text"), str)
        and isinstance(ev.get("source"), str)
        and ev.get("timestamp_utc_ms") is not None
    )


def clamp01(x: float) -> float:
    return 0.0 if x < 0.0 else (1.0 if x > 1.0 else x)


def credibility_score(ev: dict) -> float:
    """
    Defendable heuristic score:
    - provenance (trusted source) is the main signal
    - completeness adds a bit
    - recency adds a bit
    Output is rounded + stable (no float ugliness).
    """
    score = 0.0

    src = ev.get("source", "unknown")
    if src in TRUSTED_SOURCES:
        score += 0.70
    else:
        score += 0.20

    # completeness
    if ev.get("url"):
        score += 0.05
    geo = ev.get("geo") or {}
    if (
        isinstance(geo, dict)
        and geo.get("lat") is not None
        and geo.get("lon") is not None
    ):
        score += 0.05
    tags = ev.get("tags")
    if isinstance(tags, list) and len(tags) > 0:
        score += 0.05

    # recency bonus (only if timestamp is recent)
    ts = ev.get("timestamp_utc_ms")
    if isinstance(ts, int):
        age_min = (now_utc_ms() - ts) / 60000.0
        if age_min < 10:
            score += 0.10
        elif age_min < 60:
            score += 0.05

    # stable rounding
    score = clamp01(score)
    return float(f"{score:.2f}")


def main():
    logging.info("Connecting to Kafka at %s", BOOTSTRAP)

    GROUP_ID = os.getenv("GROUP_ID", "verify-events")

    consumer = KafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=[BOOTSTRAP],
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: (
            b.decode("utf-8") if b is not None else None
        ),
    )

    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        acks="all",
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
            "utf-8"
        ),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    seen: Dict[str, float] = {}
    processed = emitted = dropped_dupe = sent_dlq = 0

    logging.info(
        "Listening: IN=%s  ->  OUT=%s (DLQ=%s)", IN_TOPIC, OUT_TOPIC, DLQ_TOPIC
    )

    try:
        for msg in consumer:
            processed += 1
            ev = msg.value

            if not is_valid_event(ev):
                sent_dlq += 1
                producer.send(
                    DLQ_TOPIC,
                    key="invalid",
                    value={"error": "invalid_event", "raw": ev},
                )
                continue

            key = msg.key or event_key(ev)
            if not key:
                sent_dlq += 1
                producer.send(
                    DLQ_TOPIC,
                    key="no-key",
                    value={"error": "missing_event_id", "raw": ev},
                )
                continue

            now_s = time.time()

            if processed % CLEANUP_EVERY == 0 and seen:
                cutoff = now_s - DEDUP_TTL_SECS
                before = len(seen)
                seen = {k: t for k, t in seen.items() if t >= cutoff}
                after = len(seen)
                if after != before:
                    logging.info(
                        "Dedup cleanup: %d -> %d entries", before, after
                    )

            last = seen.get(key)
            if last is not None and (now_s - last) < DEDUP_TTL_SECS:
                dropped_dupe += 1
                continue
            seen[key] = now_s

            score = credibility_score(ev)
            verified = score >= 0.80

            out = dict(ev)
            out["credibility_score"] = score
            out["verified"] = verified
            out["processed_at_utc_ms"] = now_utc_ms()

            if verified:
                producer.send(OUT_TOPIC, key=key, value=out)
                emitted += 1
            else:
                sent_dlq += 1
                producer.send(
                    DLQ_TOPIC,
                    key=key,
                    value={"error": "low_credibility", "event": out},
                )

            if processed % 50 == 0:
                logging.info(
                    "Stats processed=%d emitted=%d dupes=%d dlq=%d dedup_mem=%d",
                    processed,
                    emitted,
                    dropped_dupe,
                    sent_dlq,
                    len(seen),
                )

    except KeyboardInterrupt:
        logging.info("Stopping…")
    finally:
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass
        try:
            consumer.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
