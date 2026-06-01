"""
/api/analytics — Gold layer aggregated analytics
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from services.lakehouse_reader import LakehouseReader

router = APIRouter()
reader = LakehouseReader()


@router.get("/moving-averages/{symbol}")
async def moving_averages(
    symbol: str,
    window_minutes: int = Query(default=5, description="Window size in minutes"),
):
    """Return moving averages from Gold layer."""
    try:
        return reader.get_moving_averages(symbol=symbol.upper(), window=window_minutes)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/volatility")
async def volatility_all(limit: int = Query(default=20)):
    """Return volatility metrics for all symbols."""
    try:
        return reader.get_volatility_metrics(limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/volatility/{symbol}")
async def volatility_symbol(symbol: str):
    """Return volatility metrics for a specific symbol."""
    try:
        return reader.get_volatility_metrics(symbol=symbol.upper(), limit=50)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/summary")
async def market_summary():
    """Return a market-level summary (breadth, average change, etc)."""
    try:
        return reader.get_market_summary()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
