import os
import time
import json
import logging
from typing import Dict, List, Optional

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "raw-events")
INTERVAL_SECS = int(os.getenv("INTERVAL_SECS", "60"))

USGS_URL = os.getenv(
    "USGS_URL",
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
)

SOURCE_NAME = "usgs"


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        acks="all",
        linger_ms=50,
        retries=5,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
            "utf-8"
        ),
        key_serializer=lambda k: k.encode("utf-8"),
    )


def fetch_usgs_events() -> List[Dict]:
    resp = requests.get(USGS_URL, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return data.get("features", [])


def usgs_feature_to_event(feature: Dict) -> Optional[Dict]:
    props = feature.get("properties", {}) or {}
    geom = feature.get("geometry", {}) or {}
    coords = geom.get("coordinates", None)

    t_ms = props.get("time")
    if t_ms is None:
        return None

    lon, lat = None, None
    if isinstance(coords, list) and len(coords) >= 2:
        lon, lat = coords[0], coords[1]

    event_id = feature.get("id") or props.get("code")
    if not event_id:
        return None

    place = props.get("place", "unknown location")
    mag = props.get("mag", None)
    url = props.get("url", None)

    text = (
        f"Earthquake reported: magnitude {mag} at {place}."
        if mag is not None
        else f"Earthquake reported at {place}."
    )

    return {
        "event_id": f"{SOURCE_NAME}:{event_id}",
        "timestamp_utc_ms": int(t_ms),
        "source": SOURCE_NAME,
        "event_type": "earthquake",
        "text": text,
        "url": url,
        "geo": {"lat": lat, "lon": lon},
        "tags": ["earthquake"],
        "raw": {
            "place": place,
            "mag": mag,
            "tsunami": props.get("tsunami"),
            "felt": props.get("felt"),
        },
    }


def main():
    while True:
        try:
            producer = make_producer()
            break
        except NoBrokersAvailable:
            logging.warning(
                "Kafka not available yet at %s. Retrying in 2s...",
                KAFKA_BOOTSTRAP,
            )
            time.sleep(2)

    logging.info(
        "Starting ingestion loop: %s → topic=%s every %ss",
        USGS_URL,
        TOPIC,
        INTERVAL_SECS,
    )

    try:
        while True:
            try:
                features = fetch_usgs_events()
                logging.info("Fetched %d USGS features", len(features))

                sent = 0
                for feat in features:
                    ev = usgs_feature_to_event(feat)
                    if ev is None:
                        continue
                    producer.send(TOPIC, key=ev["event_id"], value=ev)
                    sent += 1

                producer.flush()
                logging.info(
                    "Published %d raw events to topic '%s'", sent, TOPIC
                )

            except requests.HTTPError as e:
                logging.error(
                    "HTTP %s: %s",
                    e.response.status_code,
                    e.response.text[:200],
                )
            except Exception as e:
                logging.exception("Ingestion error: %s", e)

            time.sleep(INTERVAL_SECS)

    except KeyboardInterrupt:
        logging.info("Shutting down...")
    finally:
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
