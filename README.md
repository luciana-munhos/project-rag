# Real-Time Environmental Events RAG (Dockerized Architecture)

This project implements a real-time data pipeline for environmental events (USGS Earthquakes and NASA EONET alerts). It uses **Apache Kafka** for streaming, **Qdrant** as a vector database, and an **LLM-powered Intent Engine** to interpret, filter, and summarize disaster data.



---

## 1. Architecture
**Data Sources** (USGS/EONET) $\rightarrow$ **Kafka** (Docker) $\rightarrow$ `verify-events.py` $\rightarrow$ `index-events.py` $\rightarrow$ **Qdrant** $\rightarrow$ `ask.py` (LLM Reranking & Synthesis)

The pipeline uses a multi-stage approach:
1. **Ingestion**: Raw data is fetched and pushed to a Kafka topic.
2. **Verification**: `verify-events.py` deduplicates and validates events.
3. **Indexing**: `index-events.py` generates embeddings and stores them in Qdrant.
4. **RAG**: `ask.py` extracts intent from your query and performs spatiotemporal retrieval.

---

## 2. Requirements
- **OS**: Ubuntu / Linux
- **Software**: Python 3.10+, Docker & Docker Compose
- **API Key**: A Groq/OpenAI API key for the LLM features.

---

## 3. Setup

### Create and activate the virtual environment
bash
python3 -m venv datastream
source datastream/bin/activate
pip install -r requirements.txt

Start the Infrastructure (Docker)

This starts Kafka, Zookeeper, and Qdrant in a containerized environment.
Bash

sudo docker compose up -d

Note: Ensure your docker-compose.yml is configured for Kafka to be accessible at localhost:9092.
4. Running the Pipeline

Run each component in a separate terminal with the virtual environment activated.
Step 1: Processing and Indexing
Bash

# Terminal 1: Verification (Deduplication)
python3 verify-events.py

# Terminal 2: Indexing to Vector DB
python3 index-events.py

Step 2: Data Ingestion
Bash

# Terminal 3: USGS Earthquakes
python3 ingest-usgs.py

# Terminal 4: NASA EONET
python3 ingest-eonet.py

Step 3: Semantic Querying (RAG)

You must export your API key in the terminal session before running the query tool:
Bash

export GROQ_API_KEY="your_actual_key_here"
python3 ask.py "Show me recent floods in Asia" --topk 5 --llm

5. Maintenance & Resetting the System
Full Reset (Clean Slate)

If you want to completely wipe the system state:

    Delete the Qdrant Collection: Always run this command before stopping Docker to ensure the vector index is properly cleared:
    Bash

curl -X DELETE http://localhost:6333/collections/events

Stop and Wipe Docker Volumes:
Bash

sudo docker compose down -v

Reset Kafka Topics: To recreate topics and clear old messages:
Bash

    bash reset-topics.sh

6. Search Features (ask.py)

    Smart Intent: Uses an LLM to automatically identify if your query is "Global" or tied to a specific "Location".

    Category Filtering: Automatically applies hard filters (e.g., wildfires, earthquakes) based on user intent.

    Spatiotemporal Scoring: Reranks results using:

        Semantic Similarity: Vector distance between query and event text.

        Spatial Bias: Proximity to the detected location (using Haversine distance).

        Recency Decay: Exponential time decay (e−age/τ) to prioritize fresh events.

7. Stopping the Project
Bash

# Stop all containers
sudo docker compose stop

# Remove containers
sudo docker compose down