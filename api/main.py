"""
FastAPI — REST API Layer for Stock Lakehouse Platform
Endpoints:
  /api/stocks       — live quotes
  /api/analytics    — Gold layer aggregations
  /api/trends       — trend analysis
  /api/health       — system health
  /api/anomalies    — anomaly detection results
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from routers import stocks, analytics, trends, health, anomalies
from services.database import engine, Base


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    # Create all tables on startup
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()


# ─────────────────────────────────────────────────────────
# App Definition
# ─────────────────────────────────────────────────────────
app = FastAPI(
    title="Stock Market Lakehouse API",
    description=(
        "Real-time stock market analytics API powering the lakehouse platform. "
        "Provides access to live quotes, aggregated analytics, trend insights, "
        "and AI-driven anomaly detection."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# CORS (allow dashboard and external consumers)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────────────────
# Routers
# ─────────────────────────────────────────────────────────
app.include_router(stocks.router, prefix="/api/stocks", tags=["Stocks"])
app.include_router(analytics.router, prefix="/api/analytics", tags=["Analytics"])
app.include_router(trends.router, prefix="/api/trends", tags=["Trends"])
app.include_router(health.router, prefix="/api/health", tags=["Health"])
app.include_router(anomalies.router, prefix="/api/anomalies", tags=["Anomalies"])


@app.get("/", tags=["Root"])
async def root():
    return {
        "service": "Stock Market Lakehouse API",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs",
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=os.getenv("API_HOST", "0.0.0.0"),
        port=int(os.getenv("API_PORT", "8000")),
        reload=os.getenv("API_DEBUG", "false").lower() == "true",
        log_level="info",
    )
