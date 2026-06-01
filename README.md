# 📈 Real-Time Stock Market Lakehouse Platform

> **Production-grade, end-to-end streaming data platform** built with Kafka, PySpark, Delta Lake, FastAPI, and Streamlit. Designed for real fintech-scale analytics.

---

## 🏗️ Architecture

```
Stock Market API (Alpha Vantage / Finnhub / Yahoo Finance)
        ↓
  Kafka Producer  ──► Apache Kafka (raw-stock-events)
        ↓
  PySpark Structured Streaming
        ↓
  ┌─────────────────────────────────────────────┐
  │          Medallion Lakehouse (Delta Lake)    │
  │  Bronze (raw) → Silver (clean) → Gold (agg) │
  └─────────────────────────────────────────────┘
        ↓
  FastAPI ──────────────────────────────────────────────┐
        ↓                                               │
  Streamlit Dashboard          Grafana + Prometheus      │
        ↓                                               │
  Apache Airflow (orchestration)                        │
                                                        ↓
                                              External BI / AI tools
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| Streaming | Apache Kafka 7.5 |
| Processing | PySpark 3.4 + Structured Streaming |
| Lakehouse | Delta Lake 2.4 |
| Orchestration | Apache Airflow 2.7 |
| Warehouse | PostgreSQL 15 |
| API | FastAPI 0.104 |
| Dashboard | Streamlit 1.28 |
| Monitoring | Prometheus + Grafana |
| Containerization | Docker + Kubernetes |
| CI/CD | GitHub Actions |
| Cloud | AWS (EKS, S3, MSK) |

---

## 🚀 Quick Start (Local Development)

### Prerequisites

You need these installed on your machine:
- Docker Desktop (v24+)
- Docker Compose (v2+)
- Git
- 8GB+ RAM recommended

### Step 1 — Clone & Configure

```bash
git clone https://github.com/YOUR_USERNAME/stock-lakehouse.git
cd stock-lakehouse

# Copy env template
cp .env.example .env
```

Open `.env` and set your API provider. For development, `yahoo_finance` works **without any API key**:
```env
API_PROVIDER=yahoo_finance
```

### Step 2 — Start All Services

```bash
docker compose up -d
```

This starts: Zookeeper, Kafka, Ingestion, Spark, FastAPI, Streamlit, Airflow, PostgreSQL, Prometheus, Grafana.

### Step 3 — Verify Services

Wait ~60 seconds for all services to initialize, then check:

| Service | URL | Credentials |
|---|---|---|
| 📊 **Streamlit Dashboard** | http://localhost:8501 | — |
| 🔌 **FastAPI Docs** | http://localhost:8000/docs | — |
| 🌊 **Kafka UI** | http://localhost:8090 | — |
| ✈️ **Airflow** | http://localhost:8080 | admin / admin123 |
| 📉 **Grafana** | http://localhost:3000 | admin / admin123 |
| 🔭 **Prometheus** | http://localhost:9090 | — |
| ⚡ **Spark Master UI** | http://localhost:8082 | — |

### Step 4 — Confirm Data is Flowing

```bash
# Watch Kafka messages arrive
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic raw-stock-events \
  --from-beginning

# Check ingestion logs
docker compose logs -f ingestion

# Check Spark streaming logs
docker compose logs -f spark-streaming
```

---

## 📁 Project Structure

```
stock-lakehouse/
│
├── ingestion/              # Kafka producer + stock API clients
│   ├── producer.py         # Main ingestion loop
│   ├── stock_client.py     # Multi-provider API client
│   ├── requirements.txt
│   └── Dockerfile
│
├── spark-processing/       # PySpark Structured Streaming
│   ├── streaming_pipeline.py  # Bronze → Silver → Gold
│   └── Dockerfile
│
├── api/                    # FastAPI REST layer
│   ├── main.py
│   ├── routers/            # stocks, analytics, trends, health, anomalies
│   ├── services/           # lakehouse_reader, database
│   └── Dockerfile
│
├── dashboards/streamlit/   # Streamlit real-time dashboard
│   ├── app.py
│   └── Dockerfile
│
├── airflow/dags/           # Airflow DAGs (4 pipelines)
│
├── monitoring/             # Prometheus + Grafana config
│
├── kubernetes/             # K8s manifests for production
│
├── tests/unit/             # Pytest unit tests
│
├── .github/workflows/      # CI/CD pipelines
│
├── docker-compose.yml      # Full local stack
└── .env.example            # Environment template
```

---

## 🔑 Getting API Keys

### Option 1: Yahoo Finance (No key needed — best for dev)
```env
API_PROVIDER=yahoo_finance
```

### Option 2: Alpha Vantage (Free tier: 25 calls/day)
1. Go to: https://www.alphavantage.co/support/#api-key
2. Sign up for a free key
3. Set in `.env`:
```env
API_PROVIDER=alpha_vantage
ALPHA_VANTAGE_API_KEY=your_key_here
```

### Option 3: Finnhub (Free tier: 60 calls/min)
1. Go to: https://finnhub.io/register
2. Get your free API key
3. Set in `.env`:
```env
API_PROVIDER=finnhub
FINNHUB_API_KEY=your_key_here
```

---

## 🧪 Running Tests

```bash
# Install test dependencies
pip install pytest pytest-asyncio httpx fastapi pydantic

# Run all unit tests
pytest tests/unit/ -v

# Run with coverage
pytest tests/unit/ --cov=. --cov-report=html
```

---

## 🌊 Kafka Topics

| Topic | Description | Partitions |
|---|---|---|
| `raw-stock-events` | Raw quotes from API | 3 |
| `cleaned-stock-events` | Validated Silver data | 3 |
| `analytics-events` | Gold aggregations | 3 |
| `alerts-events` | Anomaly alerts | 3 |

---

## 🗂️ Lakehouse Layers

| Layer | Path | Description |
|---|---|---|
| **Bronze** | `/data/lakehouse/bronze/stock_events` | Raw immutable Kafka records |
| **Silver** | `/data/lakehouse/silver/stock_quotes` | Cleaned, validated, typed quotes |
| **Gold/OHLCV** | `/data/lakehouse/gold/ohlcv_1m` | 1-minute candlestick aggregations |
| **Gold/MA** | `/data/lakehouse/gold/moving_avg_5m` | 5-minute moving averages |
| **Gold/Volatility** | `/data/lakehouse/gold/volatility` | Volatility & risk metrics |

---

## 🌐 API Endpoints

```
GET /api/stocks/                  — All live quotes
GET /api/stocks/{symbol}          — Single symbol quote
GET /api/stocks/{symbol}/history  — OHLCV candlestick history
GET /api/stocks/gainers/top       — Top gaining stocks
GET /api/stocks/losers/top        — Top losing stocks

GET /api/analytics/moving-averages/{symbol}  — Moving averages
GET /api/analytics/volatility                — Volatility metrics
GET /api/analytics/summary                   — Market summary

GET /api/trends/                  — Trend direction analysis
GET /api/health/                  — System health
GET /api/health/pipeline          — Pipeline layer health
GET /api/anomalies/               — Detected anomalies
```

Full interactive docs: http://localhost:8000/docs

---

## ☁️ Production Deployment (AWS)

### Required AWS Services
- **EKS** — Kubernetes cluster
- **MSK** — Managed Kafka
- **S3** — Delta Lake storage
- **RDS** — PostgreSQL (metadata)
- **ECR** — Container registry

### Deploy Steps

```bash
# 1. Configure AWS credentials
aws configure

# 2. Create EKS cluster
eksctl create cluster --name stock-lakehouse-cluster --region us-east-1 --nodes 3

# 3. Apply Kubernetes manifests
kubectl apply -f kubernetes/deployments/all-deployments.yaml

# 4. Set secrets
kubectl create secret generic lakehouse-secrets \
  --from-literal=ALPHA_VANTAGE_API_KEY=your_key \
  --from-literal=POSTGRES_PASSWORD=your_pass \
  -n stock-lakehouse
```

---

## 🔭 Monitoring

- **Prometheus** scrapes metrics from API, Kafka, Spark, and the OS
- **Grafana** dashboards visualize pipeline health, throughput, and latency
- **Airflow** monitors DAG success/failure rates
- **ELK Stack** (optional) for centralized log aggregation

---

## 🤖 AI Features (Optional)

Enable AI-powered market summaries and anomaly detection:

```env
OPENAI_API_KEY=your_key_here
```

Then hit: `GET /api/anomalies/` for Z-score-based anomaly detection,
or extend the `/api/analytics/summary` endpoint with LLM-generated insights.

---

## 📄 License

MIT License — free to use for portfolio, learning, and production.
