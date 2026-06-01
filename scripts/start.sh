#!/bin/bash
# ============================================================
# Stock Lakehouse Platform — Quick Start Script
# ============================================================
set -e

echo "🚀 Starting Stock Market Lakehouse Platform..."

# Check .env exists
if [ ! -f ".env" ]; then
  echo "⚠️  .env not found. Copying from .env.example..."
  cp .env.example .env
  echo "✅ .env created. Edit it if needed, then re-run this script."
fi

# Check Docker is running
if ! docker info > /dev/null 2>&1; then
  echo "❌ Docker is not running. Please start Docker Desktop and try again."
  exit 1
fi

echo "🔨 Building and starting all services..."
docker compose up -d --build

echo ""
echo "⏳ Waiting 30 seconds for services to initialize..."
sleep 30

echo ""
echo "✅ All services started!"
echo ""
echo "📊 Dashboard:    http://localhost:8501"
echo "🔌 API Docs:     http://localhost:8000/docs"
echo "🌊 Kafka UI:     http://localhost:8090"
echo "✈️  Airflow:      http://localhost:8080  (admin / admin123)"
echo "📉 Grafana:      http://localhost:3000   (admin / admin123)"
echo "🔭 Prometheus:   http://localhost:9090"
echo "⚡ Spark UI:     http://localhost:8082"
echo ""
echo "📋 View logs:    docker compose logs -f"
echo "🛑 Stop:         docker compose down"
