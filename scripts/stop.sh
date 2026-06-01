#!/bin/bash
echo "🛑 Stopping Stock Lakehouse Platform..."
docker compose down
echo "✅ All services stopped. Data volumes preserved."
echo "   To also delete all data: docker compose down -v"
