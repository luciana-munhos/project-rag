import os
import time
import json
import logging
from typing import Dict, List, Optional, Tuple
import requests
import xml.etree.ElementTree as ET
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from email.utils import parsedate_to_datetime

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("TOPIC", "raw-events")
INTERVAL_SECS = int(os.getenv("INTERVAL_SECS", "120"))

# GDACS RSS feed (24h by default)
GDACS_RSS_URL = os.getenv(
    "GDACS_RSS_URL", "https://www.gdacs.org/xml/rss_24h.xml"
)

SOURCE_NAME = "gdacs"

# RSS namespaces seen in GDACS feeds
NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "gdacs": "http://www.gdacs.org",
    "geo": "http://www.w3.org/2003/01/geo/wgs84_pos#",
    "georss": "http://www.georss.org/georss",
    "dc": "http://purl.org/dc/elements/1.1/",
}


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


def _text(el: Optional[ET.Element]) -> Optional[str]:
    if el is None:
        return None
    # sometimes ElementTree includes whitespace/newlines; normalize lightly
    t = el.text
    if t is None:
        return None
    t = t.strip()
    return t if t else None


def parse_rfc822_to_ms(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    try:
        dt = parsedate_to_datetime(s)
        if dt.tzinfo is None:
            return int(dt.replace(tzinfo=time.timezone).timestamp() * 1000)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def parse_gdacs_date_to_ms(s: Optional[str]) -> Optional[int]:
    """
    Examples:
      'Wed, 31 Dec 2025 16:30:24 GMT'
    This is RFC822 too, so parsedate_to_datetime works.
    """
    return parse_rfc822_to_ms(s)


def extract_lat_lon(
    item: ET.Element,
) -> Tuple[Optional[float], Optional[float]]:
    lat = _text(item.find("geo:Point/geo:lat", NS))
    lon = _text(item.find("geo:Point/geo:long", NS))
    if lat and lon:
        try:
            return float(lat), float(lon)
        except Exception:
            pass

    pt = _text(item.find("georss:point", NS))
    if pt:
        try:
            parts = pt.split()
            if len(parts) >= 2:
                return float(parts[0]), float(parts[1])
        except Exception:
            pass

    return None, None


def fetch_gdacs_items() -> List[ET.Element]:
    resp = requests.get(GDACS_RSS_URL, timeout=20)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)

    channel = root.find("channel")
    if channel is None:
        return []
    return channel.findall("item") or []


def normalize_item(item: ET.Element) -> Optional[Dict]:
    title = _text(item.find("title"))
    desc = _text(item.find("description"))
    link = _text(item.find("link"))

    guid = _text(item.find("guid"))
    dc_subject = _text(item.find("dc:subject", NS))  # e.g. EQ1, FL1, WF1

    eventtype = _text(item.find("gdacs:eventtype", NS))  # EQ, FL, WF, TC, ...
    eventid = _text(item.find("gdacs:eventid", NS))  # numeric id in many cases
    alertlevel = _text(item.find("gdacs:alertlevel", NS))  # Green/Orange/Red
    country = _text(item.find("gdacs:country", NS))
    iso3 = _text(item.find("gdacs:iso3", NS))
    severity_text = _text(item.find("gdacs:severity", NS))
    population_text = _text(item.find("gdacs:population", NS))

    # Stable ID
    if eventtype and eventid:
        stable_id = f"{eventtype}:{eventid}"
    elif guid:
        stable_id = guid
    elif link:
        stable_id = link
    else:
        return None

    # Timestamps:
    # Prefer event start (gdacs:fromdate). Fallback to item pubDate. Fallback to now.
    fromdate = _text(item.find("gdacs:fromdate", NS))
    pubdate = _text(item.find("pubDate"))

    ts_ms = (
        parse_gdacs_date_to_ms(fromdate)
        or parse_rfc822_to_ms(pubdate)
        or int(time.time() * 1000)
    )

    lat, lon = extract_lat_lon(item)

    # Event type mapping into our schema's "event_type"
    event_type_out = {
        "EQ": "earthquakes",
        "FL": "floods",
        "TC": "tropical_cyclones",
        "WF": "wildfires",
        "VO": "volcanoes",
        "DR": "drought",
    }.get(eventtype or "", (eventtype or "disaster_event").lower())

    # Text: keep title + add extra structured hints if available
    extra_bits = []
    if alertlevel:
        extra_bits.append(f"Alert: {alertlevel}")
    if country:
        extra_bits.append(f"Country/Area: {country}")
    if severity_text:
        extra_bits.append(f"Severity: {severity_text}")
    if population_text:
        extra_bits.append(f"Impact: {population_text}")

    base_text = title or desc or ""
    if extra_bits:
        text = base_text + " " + " | ".join(extra_bits)
    else:
        text = base_text

    tags = []
    if eventtype:
        tags.append(eventtype)
    if dc_subject:
        tags.append(dc_subject)
    if alertlevel:
        tags.append(alertlevel.lower())
    if iso3:
        tags.append(iso3)

    raw_payload = {
        "title": title,
        "description": desc,
        "link": link,
        "guid": guid,
        "dc_subject": dc_subject,
        "gdacs": {
            "eventtype": eventtype,
            "eventid": eventid,
            "alertlevel": alertlevel,
            "country": country,
            "iso3": iso3,
            "severity": severity_text,
            "population": population_text,
            "fromdate": fromdate,
            "pubdate": pubdate,
        },
    }

    return {
        "event_id": f"{SOURCE_NAME}:{stable_id}",
        "timestamp_utc_ms": int(ts_ms),
        "source": SOURCE_NAME,
        "event_type": event_type_out,
        "text": text,
        "url": link,
        "geo": {"lat": lat, "lon": lon},
        "tags": tags,
        "raw": raw_payload,
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
        "GDACS ingestor started: %s → topic=%s every %ss",
        GDACS_RSS_URL,
        TOPIC,
        INTERVAL_SECS,
    )

    try:
        while True:
            try:
                items = fetch_gdacs_items()
                logging.info("Fetched %d GDACS items", len(items))

                sent = 0
                for it in items:
                    ev = normalize_item(it)
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
                    (e.response.text or "")[:200],
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
