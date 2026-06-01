"""Health router — /api/health"""
import os, time
from fastapi import APIRouter
from services.lakehouse_reader import LakehouseReader

router = APIRouter()
reader = LakehouseReader()

@router.get("/")
async def health_check():
    checks = {
        layer: os.path.exists(path)
        for layer, path in [
            ("bronze", os.getenv("BRONZE_PATH", "/data/lakehouse/bronze")),
            ("silver", os.getenv("SILVER_PATH", "/data/lakehouse/silver")),
            ("gold",   os.getenv("GOLD_PATH",   "/data/lakehouse/gold")),
        ]
    }
    return {
        "status": "healthy" if all(checks.values()) else "degraded",
        "timestamp": time.time(),
        "checks": checks,
    }

@router.get("/pipeline")
async def pipeline_health():
    try:
        return reader.get_pipeline_health()
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}
