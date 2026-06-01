"""Trends router — /api/trends"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from services.lakehouse_reader import LakehouseReader

router = APIRouter()
reader = LakehouseReader()

@router.get("/")
async def get_trends(
    hours: int = Query(default=6, le=48),
    symbol: Optional[str] = Query(default=None),
):
    try:
        return reader.get_trends(hours=hours, symbol=symbol)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
