"""
/api/stocks — Live stock quote endpoints
"""

from __future__ import annotations

from typing import Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from services.lakehouse_reader import LakehouseReader

router = APIRouter()
reader = LakehouseReader()


# ─────────────────────────────────────────────────────────
# Response models
# ─────────────────────────────────────────────────────────
class StockQuoteResponse(BaseModel):
    symbol: str
    price: float
    open: float
    high: float
    low: float
    prev_close: float
    volume: int
    change: float
    change_pct: float
    timestamp: str
    provider: str


class StockListResponse(BaseModel):
    count: int
    data: list[StockQuoteResponse]
    as_of: str


# ─────────────────────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────────────────────
@router.get("/", response_model=StockListResponse)
async def list_stocks(
    limit: int = Query(default=50, le=500, description="Max records to return"),
    symbol: Optional[str] = Query(default=None, description="Filter by symbol"),
):
    """Return latest quotes for all tracked symbols (or filtered by symbol)."""
    try:
        data = reader.get_latest_quotes(symbol=symbol, limit=limit)
        return StockListResponse(
            count=len(data),
            data=data,
            as_of=datetime.utcnow().isoformat(),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/{symbol}", response_model=StockQuoteResponse)
async def get_stock(symbol: str):
    """Return the latest quote for a specific symbol."""
    data = reader.get_latest_quotes(symbol=symbol.upper(), limit=1)
    if not data:
        raise HTTPException(
            status_code=404, detail=f"No data found for symbol: {symbol}"
        )
    return data[0]


@router.get("/{symbol}/history")
async def get_stock_history(
    symbol: str,
    hours: int = Query(default=24, le=168, description="Hours of history"),
    interval: str = Query(default="1m", description="Candle interval: 1m, 5m, 1h"),
):
    """Return OHLCV candlestick history from Gold layer."""
    try:
        data = reader.get_ohlcv_history(
            symbol=symbol.upper(), hours=hours, interval=interval
        )
        return {
            "symbol": symbol.upper(),
            "interval": interval,
            "count": len(data),
            "data": data,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/gainers/top")
async def top_gainers(limit: int = Query(default=10, le=50)):
    """Return top gaining stocks by percentage change."""
    try:
        return reader.get_top_movers(direction="gainers", limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/losers/top")
async def top_losers(limit: int = Query(default=10, le=50)):
    """Return top losing stocks by percentage change."""
    try:
        return reader.get_top_movers(direction="losers", limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
