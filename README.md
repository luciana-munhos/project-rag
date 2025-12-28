# Real-Time Environmental Events RAG

This project implements a small real-time pipeline for environmental events (e.g. earthquakes and NASA EONET alerts) using **Kafka** for streaming, **Qdrant** as a vector database, and a retrieval + LLM query interface.

The system ingests events, verifies them, indexes them into Qdrant, and allows semantic querying over recent events.

---

## Architecture

USGS / EONET -> Kafka (raw events) -> verify-events.py -> Kafka (verified events) ->
index-events.py -> Qdrant (vectors) -> ask.py

---

## Requirements

- Ubuntu / Linux (tested on Ubuntu)
- Python 3.10+
- Docker + Docker Compose
- **Local Kafka installation** (tested with Kafka 3.8.0, Scala 2.13)

> Kafka and ZooKeeper are started **locally**, not via Docker.  
> Docker Compose is only used for Qdrant.

---

## Repository contents

- `ingest-usgs.py` – ingests earthquake data from USGS into Kafka  
- `ingest-eonet.py` – ingests NASA EONET events into Kafka  
- `verify-events.py` – consumes raw events and outputs verified events  
- `index-events.py` – indexes verified events into Qdrant  
- `ask.py` – semantic querying (and optional LLM response via Groq)  
- `docker-compose.yml` – runs Qdrant  
- `reset-topics.sh` – Ubuntu-only helper script to delete/recreate Kafka topics  
- `eonet_seen_ids.json` – local state file to avoid re-ingesting duplicates  

---

## Setup

### 1) Create and activate a virtual environment

From the project root:

bash:
python3 -m venv datastream
source datastream/bin/activate
pip install -r requirements.txt

You should see (datastream) in your shell prompt before running any Python script.

### 2) Start ZooKeeper (Terminal 1)

Kafka must be installed locally. Example path:

cd ~/kafka_2.13-3.8.0
bin/zookeeper-server-start.sh config/zookeeper.properties

### 3) Start Kafka broker (Terminal 2)
cd ~/kafka_2.13-3.8.0
bin/kafka-server-start.sh config/server.properties

### 4) Start Qdrant (Terminal 3)

From the project directory:
sudo docker compose up -d

Qdrant will be available at:
REST API: http://localhost:6333
gRPC: localhost:6334

Data is persisted in a Docker volume (qdrant_data).

## Running the pipeline

Each component should be run in a separate terminal with the virtual environment activated.

### Terminal 4 - Verify events (raw → verified)
source datastream/bin/activate
python3 verify-events.py

### Terminal 5 - Index events into Qdrant
source datastream/bin/activate
python3 index-events.py

### Terminal 6 - Ingest USGS data
source datastream/bin/activate
python3 ingest-usgs.py

### Terminal 7 - Ingest EONET data
source datastream/bin/activate
python3 ingest-eonet.py

You may optionally use -u (python3 -u ingest-eonet.py) to force unbuffered output and see logs immediately, but it is not required.

## Querying the data

Once events are indexed in Qdrant:

source datastream/bin/activate
python3 ask.py "earthquakes in California" --topk 8 --minutes 60

## LLM configuration

If ask.py uses Groq’s OpenAI-compatible API, set your key as an environment variable:

export GROQ_API_KEY="YOUR_GROQ_API_KEY"


The key is not stored in the repository and must be provided by each user.

## Kafka topic reset (optional)

If you want to start from a clean Kafka state:

bash reset-topics.sh

(!) This script is Ubuntu-specific and assumes a local Kafka installation.

## Local state and persistence

- eonet_seen_ids.json tracks which EONET events were already ingested.
- If Qdrant is reset but this file is not, ingestion may skip events.

To fully reset ingestion state:

echo "[]" > eonet_seen_ids.json

## Stopping everything
### Stop Qdrant
sudo docker compose down

### Stop Kafka and ZooKeeper
(Ctrl+C in their respective terminals)

