"""Anomalies router — /api/anomalies"""
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from services.lakehouse_reader import LakehouseReader

router = APIRouter()
reader = LakehouseReader()

@router.get("/")
async def list_anomalies(
    hours: int = Query(default=1, le=24),
    symbol: Optional[str] = Query(default=None),
):
    try:
        return reader.get_anomalies(hours=hours, symbol=symbol)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
