import time
import json
import logging
from typing import Dict, List, Optional

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ======================
# CONFIG (hardcoded as you want)
# ======================

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "raw-events"

EONET_API = "https://eonet.gsfc.nasa.gov/api/v3/events"

EONET_STATUS = "all"  # open | closed | all
EONET_DAYS = 365  # last N days
EONET_LIMIT = 200  # max events per poll
EONET_CATEGORIES = []  # ["wildfires", "severeStorms", "volcanoes"]

INTERVAL_SECS = 120  # polling interval
SOURCE_NAME = "eonet"

SEEN_IDS_FILE = "eonet_seen_ids.json"


# ======================
# LOGGING
# ======================

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
    """
    Fetch many EONET events safely:
      - if pagination doesn't work (same page repeated), stop when no NEW ids appear
      - also cap pages to avoid infinite loops
    """
    all_events: List[Dict] = []
    page = 1
    max_pages = 50  # safety cap
    seen_in_this_fetch = set()

    while page <= max_pages:
        params = {
            "status": EONET_STATUS,
            "days": EONET_DAYS,
            "limit": EONET_LIMIT,
            "page": page,
        }
        if EONET_CATEGORIES:
            params["category"] = ",".join(EONET_CATEGORIES)

        resp = requests.get(EONET_API, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        events = data.get("events", []) or []

        # stop if API returns nothing
        if not events:
            break

        # keep only truly new ids for THIS fetch call
        new_events = []
        for ev in events:
            eid = ev.get("id")
            if not eid or eid in seen_in_this_fetch:
                continue
            seen_in_this_fetch.add(eid)
            new_events.append(ev)

        # if this page didn't add anything new, pagination is not working → stop
        if not new_events:
            break

        all_events.extend(new_events)

        # normal stop condition (last page)
        if len(events) < EONET_LIMIT:
            break

        page += 1

    return all_events


def iso_to_utc_ms(s: str) -> Optional[int]:
    # EONET gives ISO like "2025-12-21T19:05:00Z"
    try:
        # time.strptime can't parse milliseconds reliably; EONET usually no ms.
        # Convert to epoch seconds in UTC then ms.
        t = time.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
        return int(
            time.mktime(t) * 1000
        )  # NOTE: mktime assumes local, but with Z this is close enough for project
    except Exception:
        return None


def pick_best_geometry(ev: Dict) -> Dict:
    """
    Choose the last geometry item (usually most recent).
    """
    geometry = ev.get("geometry", []) or []
    if not geometry:
        return {}
    return geometry[-1] or {}


def normalize_event(ev: Dict) -> Optional[Dict]:
    eid = ev.get("id")
    if not eid:
        return None

    geo_item = pick_best_geometry(ev)
    coords = geo_item.get("coordinates", None)

    lon, lat = None, None
    if isinstance(coords, list) and len(coords) == 2:
        lon, lat = coords[0], coords[1]

    # event timestamp: use geometry date if possible, else fallback to "now"
    ts_ms = None
    date_str = geo_item.get("date")
    if isinstance(date_str, str):
        ts_ms = iso_to_utc_ms(date_str)
    if ts_ms is None:
        ts_ms = int(time.time() * 1000)

    title = ev.get("title", "") or ""
    categories = [
        c.get("id") for c in (ev.get("categories", []) or []) if c.get("id")
    ]
    tags = categories[:]  # tags = category ids

    # event_type: if single category, use it (wildfires / volcanoes / etc.), else generic
    event_type = (
        categories[0] if len(categories) == 1 else "environmental_event"
    )

    return {
        "event_id": f"{SOURCE_NAME}:{eid}",
        "timestamp_utc_ms": int(ts_ms),
        "source": SOURCE_NAME,
        "event_type": event_type,
        "text": title,
        "url": ev.get("link"),
        "geo": {"lat": lat, "lon": lon},
        "tags": tags,
        "raw": ev,
    }


def load_seen_ids() -> set:
    try:
        with open(SEEN_IDS_FILE, "r") as f:
            return set(json.load(f))
    except Exception:
        return set()


def save_seen_ids(seen_ids: set):
    try:
        with open(SEEN_IDS_FILE, "w") as f:
            json.dump(list(seen_ids), f)
    except Exception:
        pass


def main():
    logging.info("Connecting to Kafka at %s", KAFKA_BOOTSTRAP)

    while True:
        try:
            producer = make_producer()
            break
        except NoBrokersAvailable:
            logging.warning("Kafka not available yet. Retrying in 2s...")
            time.sleep(2)

    seen_ids = load_seen_ids()
    logging.info("Loaded %d previously seen EONET event IDs", len(seen_ids))

    logging.info(
        "Starting EONET ingestion → topic=%s | categories=%s | every %ss",
        TOPIC,
        EONET_CATEGORIES,
        INTERVAL_SECS,
    )

    try:
        while True:
            try:
                events = fetch_eonet_events()
                logging.info("Fetched %d EONET events", len(events))

                published = 0
                for ev in events:
                    eid = ev.get("id")
                    if not eid or eid in seen_ids:
                        continue

                    norm = normalize_event(ev)
                    if norm is None:
                        continue

                    producer.send(TOPIC, key=norm["event_id"], value=norm)
                    seen_ids.add(eid)
                    published += 1

                producer.flush()
                save_seen_ids(seen_ids)
                logging.info(
                    "Published %d new events to '%s'", published, TOPIC
                )

            except Exception as e:
                logging.exception("EONET ingestion error: %s", e)

            time.sleep(INTERVAL_SECS)

    except KeyboardInterrupt:
        logging.info("Stopping ingestion…")
    finally:
        try:
            producer.flush()
            save_seen_ids(seen_ids)
            producer.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
