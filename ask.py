#!/usr/bin/env python3
import argparse
import datetime as dt
import math
import os
import re
from dataclasses import dataclass
from datetime import timezone
from typing import Any, Dict, List, Optional, Tuple

from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue, Range
from sentence_transformers import SentenceTransformer

from openai import OpenAI
import json


# Optional: location geocoding
# pip install geopy
try:
    from geopy.geocoders import Nominatim
except Exception:
    Nominatim = None


# -----------------------------
# Time utilities
# -----------------------------
def now_ms() -> int:
    return int(dt.datetime.now(timezone.utc).timestamp() * 1000)


def ms_to_utc_str(ms: Optional[int]) -> str:
    if ms is None:
        return "N/A"
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


def age_minutes(ts_ms: Optional[int]) -> Optional[float]:
    if ts_ms is None:
        return None
    return max(0.0, (now_ms() - int(ts_ms)) / 60000.0)


# -----------------------------
# Scoring helpers
# -----------------------------
def exp_decay_age(ts_ms: int, tau_minutes: float) -> float:
    """Returns ~1 for very recent events, decays with age."""
    age_min = max(0.0, (now_ms() - ts_ms) / 60000.0)
    return math.exp(-age_min / tau_minutes)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance in km between 2 lat/lon points."""
    R = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def spatial_score(distance_km: float, sigma_km: float) -> float:
    """
    Soft spatial relevance: 1 near the query location, smoothly decays with distance.
    sigma_km controls how forgiving it is (bigger = less restrictive).
    """
    return math.exp(-distance_km / sigma_km)


# -----------------------------
# Payload extraction
# -----------------------------
def extract_place(payload: Dict[str, Any]) -> str:
    """
    Tries multiple locations where place might be stored.
    Works if you store:
      - payload["place"]
      - payload["raw"]["place"]
      - parse from payload["text"]
    """
    if payload.get("place"):
        return str(payload["place"])

    raw = payload.get("raw")
    if isinstance(raw, dict) and raw.get("place"):
        return str(raw["place"])

    text = payload.get("text") or payload.get("doc") or ""
    # USGS-style text: "... at <place>."
    m = re.search(r"\bat\s+(.+?)(?:\.\s*\(|\.$)", text)
    if m:
        return m.group(1).strip()

    return "N/A"


def extract_text(payload: Dict[str, Any]) -> str:
    text = payload.get("text") or payload.get("doc") or ""
    if isinstance(text, str):
        return text.strip()
    return ""


def extract_geo(payload: dict) -> Tuple[Optional[float], Optional[float]]:
    """
    Tries multiple common shapes:
    - payload["geo"] = {"lat": .., "lon": ..}  (your case)
    - payload["geo"] = {"lat": .., "lng": ..}
    - payload has top-level "lat"/"lon"
    - payload["location"] = {"lat": .., "lon": ..}
    """
    if not payload:
        return None, None

    # 1) Your main case
    geo = payload.get("geo")
    if isinstance(geo, dict):
        lat = geo.get("lat")
        lon = geo.get("lon", geo.get("lng"))
        try:
            if lat is not None and lon is not None:
                return float(lat), float(lon)
        except (TypeError, ValueError):
            pass

    # 2) sometimes stored as top-level
    for lat_key, lon_key in [("lat", "lon"), ("latitude", "longitude")]:
        if lat_key in payload and lon_key in payload:
            try:
                return float(payload[lat_key]), float(payload[lon_key])
            except (TypeError, ValueError):
                pass

    # 3) alternate nesting
    loc = payload.get("location")
    if isinstance(loc, dict):
        lat = loc.get("lat")
        lon = loc.get("lon", loc.get("lng"))
        try:
            if lat is not None and lon is not None:
                return float(lat), float(lon)
        except (TypeError, ValueError):
            pass

    return None, None


# -----------------------------
# Qdrant filter
# -----------------------------
def build_filter(
    minutes: Optional[int], source: Optional[str], event_type: Optional[str]
) -> Optional[Filter]:
    must = []

    if minutes is not None:
        min_ts = now_ms() - minutes * 60 * 1000
        must.append(
            FieldCondition(key="timestamp_utc_ms", range=Range(gte=min_ts))
        )

    if source:
        must.append(
            FieldCondition(key="source", match=MatchValue(value=source))
        )

    if event_type and event_type != "Not Mentioned":
        must.append(
            FieldCondition(
                key="event_type", match=MatchValue(value=event_type)
            )
        )

    if not must:
        return None

    return Filter(must=must)


# -----------------------------
# Location extraction + geocoding
# -----------------------------


def geocode_place(place: str) -> Optional[Tuple[str, float, float]]:
    """
    Convert a place name into (display_name, lat, lon) using Nominatim via geopy.
    Requires: pip install geopy
    """
    if not place or Nominatim is None:
        return None
    try:
        geolocator = Nominatim(user_agent="project-rag-ask")
        loc = geolocator.geocode(place, timeout=6)
        if loc:
            return (
                loc.address or place,
                float(loc.latitude),
                float(loc.longitude),
            )
    except Exception:
        return None
    return None


def parse_latlon(s: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    """
    Parse 'lat,lon' from a string.
    """
    if not s:
        return None, None
    try:
        a, b = s.split(",", 1)
        return float(a.strip()), float(b.strip())
    except Exception:
        return None, None


def build_context(rows):
    # rows: list of (final, sim, sscore, tscore, dist_km, r)
    lines = []
    latest = None
    current_time_str = dt.datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M UTC"
    )

    for final, sim, sscore, tscore, dist_km, r in rows:
        p = r.payload or {}
        ts = p.get("timestamp_utc_ms")
        if ts is not None:
            latest = ts if (latest is None or ts > latest) else latest

        lines.append(
            f"- time={ms_to_utc_str(ts)} | source={p.get('source','N/A')} | type={p.get('event_type','N/A')}\n"
            f"  text={extract_text(p)}\n"
            f"  url={p.get('url','N/A')}"
        )
    return latest, current_time_str, "\n".join(lines)


# -----------------------------
# Intent Extraction (Query Engineering)
# -----------------------------
def extract_query_intent(query: str, client: OpenAI) -> Dict[str, Any]:
    """Uses LLM to extract category and location (uses 'Global' if no specific place is mentioned) from the user query."""
    val_categories = [
        "drought",
        "dustHaze",
        "earthquakes",
        "floods",
        "landslides",
        "manmade",
        "seaLakeIce",
        "severeStorms",
        "snow",
        "tempExtremes",
        "volcanoes",
        "waterColor",
        "wildfires",
    ]

    system_prompt = f"""
    Extract search parameters from the user query.
    Valid categories (IDs): {val_categories}
    
    Return ONLY a JSON object:
    {{
      "category": "one of the valid IDs or 'Not Mentioned' if it doesn't fit any ID",
      "location": "the place name, or 'Global' if no specific place is mentioned",
      "search_query": "clean keyword version of the query"
    }}
    """
    try:
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": query},
            ],
            response_format={"type": "json_object"},
        )
        content = resp.choices[0].message.content
        return json.loads(content)
    except Exception as e:
        print(f"DEBUG ERROR: {e}")
        # Fallback if LLM fails
        return {
            "category": "Not Mentioned",
            "location": "Global",
            "search_query": query,
        }


# -----------------------------
# Main
# -----------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Recency + spatial aware semantic retrieval over streaming events"
    )
    parser.add_argument("query", type=str, help="Search query")
    parser.add_argument("--topk", type=int, default=5)
    parser.add_argument(
        "--minutes", type=int, default=None, help="Hard time filter (optional)"
    )
    parser.add_argument("--source", type=str, default=None)
    parser.add_argument("--type", type=str, default=None)

    # Optional override if you want to avoid geocoding:
    parser.add_argument(
        "--qgeo",
        type=str,
        default=None,
        help='Optional query coordinates override as "lat,lon" (e.g. "36.77,-119.41")',
    )

    # Scoring knobs
    parser.add_argument(
        "--overfetch",
        type=int,
        default=10,
        help="Retrieve topk*overfetch semantic candidates then rerank",
    )
    parser.add_argument(
        "--sigma-km",
        type=float,
        default=2000.0,
        help="Spatial softness (higher = less restrictive)",
    )
    parser.add_argument(
        "--tau-min",
        type=float,
        default=360.0,
        help="Recency decay time constant in minutes",
    )
    parser.add_argument(
        "--w-space",
        type=float,
        default=0.25,
        help="How much spatial bias affects semantic score",
    )
    parser.add_argument(
        "--w-time",
        type=float,
        default=0.05,
        help="Small bonus weight for recency (tie-breaker)",
    )
    parser.add_argument(
        "--max-km",
        type=float,
        default=None,
        help="If set, prefer results within this distance from detected query location",
    )
    parser.add_argument(
        "--min-nearby",
        type=int,
        default=None,
        help="Minimum number of nearby results required to apply the --max-km gate (default: topk//2)",
    )
    parser.add_argument(
        "--llm", action="store_true", help="Generate final answer with OpenAI"
    )
    args = parser.parse_args()

    qdrant_url = os.getenv("QDRANT_URL", "http://localhost:6333")
    collection = os.getenv("QDRANT_COLLECTION", "events")
    model_name = os.getenv("EMB_MODEL", "all-MiniLM-L6-v2")

    print(f"Qdrant: {qdrant_url}  collection={collection}")
    print(f"Embedding model: {model_name}")
    print(f"Query: {args.query}\n")

    ## Adding an LLM to get the query information
    client = OpenAI(
        api_key=os.environ.get("GROQ_API_KEY", "TO_REPLACE"),
        base_url="https://api.groq.com/openai/v1",
    )

    intent = extract_query_intent(args.query, client)

    # Let's extract the variables we need
    extracted_loc = intent.get("location", "Global")
    final_type = intent.get("category")
    search_terms = intent.get("search_query", args.query)

    print(f"Query Intent: Category={final_type}, Location={extracted_loc}")

    model = SentenceTransformer(model_name)
    qdrant = QdrantClient(url=qdrant_url)

    query_vec = model.encode(search_terms).tolist()
    flt = build_filter(args.minutes, args.source, final_type)

    # 1) Retrieve by semantics (dominant)
    limit = max(args.topk * args.overfetch, args.topk)
    response = qdrant.query_points(
        collection_name=collection,
        query=query_vec,
        limit=limit,
        query_filter=flt,
        with_payload=True,
        with_vectors=False,
    )

    points = response.points
    if not points:
        print("No results found.")
        return

    # 2) Determine query lat/lon (reference point for distance)
    qlat, qlon = parse_latlon(args.qgeo)

    # Only attempt geocoding if NOT Global and no override is provided
    if qlat is None and extracted_loc.lower() != "global":
        loc = geocode_place(extracted_loc)
        if loc:
            _, qlat, qlon = loc
            print(f"Detected location: {extracted_loc} -> ({qlat}, {qlon})")

    # 3) First compute features (sim, time, distance, spatial score) for all candidates
    scored: List[Tuple[float, float, float, float, Optional[float], Any]] = []
    for i, r in enumerate(points, start=1):
        payload = r.payload or {}
        sim = float(r.score)

        ts = payload.get("timestamp_utc_ms")
        tscore = (
            exp_decay_age(int(ts), args.tau_min) if ts is not None else 0.0
        )

        lat, lon = extract_geo(payload)

        dist_km = None
        if (
            (qlat is not None)
            and (qlon is not None)
            and (lat is not None)
            and (lon is not None)
        ):
            dist_km = haversine_km(qlat, qlon, lat, lon)
            sscore = spatial_score(dist_km, args.sigma_km)
        else:
            sscore = 1.0  # neutral if we can't compute distance

        # NOTE: final is computed later (after optional gating)
        scored.append((0.0, sim, sscore, tscore, dist_km, r))

    # 4) OPTIONAL: spatial gate (only if we have query coords + user asked for max-km)
    gated = scored
    if (args.max_km is not None) and (qlat is not None) and (qlon is not None):
        min_nearby = (
            args.min_nearby
            if args.min_nearby is not None
            else max(1, args.topk // 2)
        )

        nearby = [
            t for t in scored if (t[4] is not None) and (t[4] <= args.max_km)
        ]

        if len(nearby) >= min_nearby:
            gated = nearby
            print(
                f"Applied spatial gate: keeping {len(nearby)}/{len(scored)} results within {args.max_km:.0f} km\n"
            )
        else:
            # not enough nearby evidence; don't over-restrict
            print(
                f"Spatial gate NOT applied (only {len(nearby)}/{min_nearby} results within {args.max_km:.0f} km). "
                "Showing best global matches.\n"
            )

    # 5) Compute final score and rank (similarity dominant, space biases it, time small bonus)
    reranked: List[Tuple[float, float, float, float, Optional[float], Any]] = (
        []
    )
    for _, sim, sscore, tscore, dist_km, r in gated:
        biased_sim = sim * ((1.0 - args.w_space) + args.w_space * sscore)
        final = biased_sim + args.w_time * tscore
        reranked.append((final, sim, sscore, tscore, dist_km, r))

    reranked.sort(key=lambda x: x[0], reverse=True)
    reranked = reranked[: args.topk]

    # Output
    for i, (final, sim, sscore, tscore, dist_km, r) in enumerate(reranked, 1):
        p = r.payload or {}
        lat, lon = extract_geo(p)
        place = extract_place(p)
        text = extract_text(p)

        print(
            f"{i}) final_score={final:.4f}  sim={sim:.3f}  space={sscore:.3f}  time={tscore:.3f}"
        )
        print(f"   type:     {p.get('event_type', 'N/A')}")
        print(f"   source:   {p.get('source', 'N/A')}")
        print(f"   time:     {ms_to_utc_str(p.get('timestamp_utc_ms'))}")
        age_min = age_minutes(p.get("timestamp_utc_ms"))
        if age_min is not None:
            print(f"   age:      {age_min:.1f} min")

        print(f"   place:    {place}")
        if (lat is not None) and (lon is not None):
            print(f"   location: lat={lat}, lon={lon}")
        else:
            print("   location: N/A")

        if dist_km is not None:
            print(f"   distance: {dist_km:.1f} km")

        print(f"   id:       {p.get('event_id', 'N/A')}")
        print(f"   url:      {p.get('url', 'N/A')}")
        print(f"   text:     {text}")
        print("")

    # 6) Optional LLM final answer
    if args.llm:
        latest_ts, now_str, ctx = build_context(reranked)

        asof = ms_to_utc_str(latest_ts) if latest_ts is not None else "N/A"

        prompt = f"""You are a real-time disaster/event monitor.

    Current System Time: {now_str}
    Latest Event in Database: {asof}

    User question: {args.query}

    INSTRUCTIONS:
    1. Start your response by clearly stating: "Report generated at {now_str}."
    2. Summarize the situation  based ONLY on the context below. If the context is insufficient or off-topic, say so and suggest widening the time window.
    3. Begin the summary with the phrase: "As of {asof}, ..."
    4. If the latest data ({asof}) is older than 24 hours relative to {now_str}, you have to state that there are no new reports in the last 24 hours."
    5. Provide the response in the following format:
    - A short summary (starting as instructed above).
    - Bullet points of the key confirmed facts.
    - A "Sources" list with the URLs used from the context.

    Context:
    {ctx}
    """
        resp = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
        )
        print(resp.choices[0].message.content)
        return


if __name__ == "__main__":
    main()
