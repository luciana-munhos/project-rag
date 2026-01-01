import time
import json
import logging
from typing import Dict, List, Optional
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Config
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "raw-events"
EONET_API = "https://eonet.gsfc.nasa.gov/api/v3/events"
EONET_STATUS = "all"
EONET_DAYS = 365
EONET_LIMIT = 200
INTERVAL_SECS = 120
SOURCE_NAME = "eonet"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        acks="all",
        linger_ms=50,
        retries=5,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
            "utf-8"
        ),
        key_serializer=lambda k: k.encode("utf-8"),
    )


def fetch_eonet_events() -> List[Dict]:
    all_events: List[Dict] = []
    page = 1
    max_pages = 10
    seen_in_this_fetch = set()

    while page <= max_pages:
        params = {
            "status": EONET_STATUS,
            "days": EONET_DAYS,
            "limit": EONET_LIMIT,
            "page": page,
        }
        resp = requests.get(EONET_API, params=params, timeout=20)
        resp.raise_for_status()
        events = resp.json().get("events", []) or []

        if not events:
            break

        new_events = []
        for ev in events:
            eid = ev.get("id")
            if not eid or eid in seen_in_this_fetch:
                continue
            seen_in_this_fetch.add(eid)
            new_events.append(ev)

        if not new_events:
            break
        all_events.extend(new_events)
        if len(events) < EONET_LIMIT:
            break
        page += 1
    return all_events


def iso_to_utc_ms(s: str) -> Optional[int]:
    try:
        # EONET gives "2025-12-21T19:05:00Z"
        t = time.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
        return int(time.mktime(t) * 1000)
    except:
        return None


def normalize_event(ev: Dict) -> Optional[Dict]:
    eid = ev.get("id")
    if not eid:
        return None

    geometry = ev.get("geometry", []) or []
    geo_item = geometry[-1] if geometry else {}
    coords = geo_item.get("coordinates")
    lon, lat = (
        (coords[0], coords[1])
        if isinstance(coords, list) and len(coords) == 2
        else (None, None)
    )

    # timestamp logic
    ts_ms = None
    date_str = geo_item.get("date")
    if isinstance(date_str, str):
        ts_ms = iso_to_utc_ms(date_str)
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)

    categories = [c.get("id") for c in ev.get("categories", []) if c.get("id")]

    return {
        "event_id": f"{SOURCE_NAME}:{eid}",
        "timestamp_utc_ms": int(ts_ms),
        "source": SOURCE_NAME,
        "event_type": categories[0] if categories else "environmental_event",
        "text": ev.get("title", ""),
        "url": ev.get("link"),
        "geo": {"lat": lat, "lon": lon},
        "tags": categories,
        "raw": ev,
    }


def main():
    while True:
        try:
            producer = make_producer()
            break
        except NoBrokersAvailable:
            logging.warning("Kafka not available yet. Retrying...")
            time.sleep(2)

    logging.info("EONET Ingestor started (Corrected Pagination + Stateless)")

    while True:
        try:
            events = fetch_eonet_events()
            published = 0
            for ev in events:
                norm = normalize_event(ev)
                if norm:
                    producer.send(TOPIC, key=norm["event_id"], value=norm)
                    published += 1
            producer.flush()
            logging.info(f"Published {published} events to Kafka.")
        except Exception as e:
            logging.error(f"Error: {e}")
        time.sleep(INTERVAL_SECS)


if __name__ == "__main__":
    main()
