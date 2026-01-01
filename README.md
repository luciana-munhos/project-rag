# Real-Time Environmental Events RAG (Dockerized Architecture)

This project implements a real-time data pipeline for environmental and disaster events
(USGS Earthquakes, NASA EONET alerts, and GDACS disaster notifications).
It uses **Apache Kafka** for streaming, **Qdrant** as a vector database, and an
**LLM-powered RAG engine** to interpret, filter, and summarize events.

---

## 1. Architecture

**Data Sources (USGS / EONET / GDACS)** → **Kafka (Docker)** → `verify-events.py`
→ `index-events.py` → **Qdrant** → `ask.py` (LLM-based Retrieval & Synthesis)

The pipeline uses a multi-stage approach:

1. **Ingestion**  
   Raw data is fetched from multiple public sources and published to Kafka:
   - `ingest-usgs.py`
   - `ingest-eonet.py`
   - `ingest-gdacs.py`

2. **Verification**  
   `verify-events.py` consumes raw events, deduplicates and validates them,
   and republishes verified events to a downstream Kafka topic.

3. **Indexing**  
   `index-events.py` consumes verified events, generates embeddings,
   and upserts them into Qdrant using a *commit-after-write* offset strategy.

4. **RAG Querying**  
   `ask.py` extracts intent from user queries, performs spatiotemporal retrieval
   over the vector database, and synthesizes a natural-language answer with an LLM.

---

## 2. Requirements

- **OS**: Ubuntu / Linux
- **Software**: Python 3.10+, Docker, Docker Compose
- **API Key**: A Groq API key for LLM-based querying

---

## 3. Setup

### Create and activate the virtual environment

python3 -m venv datastream
source datastream/bin/activate
pip install -r requirements.txt


### Start the Infrastructure (Docker)

This starts Kafka, Zookeeper, and Qdrant:
sudo docker compose up -d

Note: Ensure your docker-compose.yml is configured for Kafka to be accessible at localhost:9092.

## 4. Running the Pipeline

Run each component in a separate terminal with the virtual environment activated.

### Step 1: Processing and Indexing
Bash

Terminal 1: Verification (Deduplication)
python3 verify-events.py

Terminal 2: Indexing to Vector DB
python3 index-events.py

## Step 2: Data Ingestion
Terminal 3: USGS Earthquakes
python3 ingest-usgs.py

Terminal 4: NASA EONET
python3 ingest-eonet.py

## Step 3: Semantic Querying (RAG)

You must export your API key in the terminal session before running the query tool:
export GROQ_API_KEY="your_actual_key_here"

Run a query:
python3 ask.py "Show me recent floods in Asia" --topk 5 --llm

## 5. Maintenance & Resetting the System
Full Reset (Clean Slate)

If you want to completely wipe the system state:

    Delete the Qdrant Collection: Always run this command before stopping Docker to ensure the vector index is properly cleared:
    Bash

curl -X DELETE http://localhost:6333/collections/events

Stop and Wipe Docker Volumes:
sudo docker compose down -v

Reset Kafka Topics: To recreate topics and clear old messages:
bash reset-topics.sh

## 6. Search Features (ask.py)

- Smart Intent: Uses an LLM to automatically identify if your query is "Global" or tied to a specific "Location".

- Category Filtering: Automatically applies hard filters (e.g., wildfires, earthquakes) based on user intent.

- Spatiotemporal Scoring: Reranks results using: Semantic Similarity: Vector distance between query and event text;
Spatial Bias: Proximity to the detected location (using Haversine distance); Recency Decay: Exponential time decay (e−age/τ) to prioritize fresh events.

## 7. Stopping the Project
Stop all containers
sudo docker compose stop

Remove containers
sudo docker compose down