"""
LakehouseReader
Reads processed data from Delta Lake layers for the API layer.
Falls back to mock data if Delta tables aren't available yet.
"""

from __future__ import annotations

import logging
import os
import random
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

SILVER_PATH = os.getenv("SILVER_PATH", "/data/lakehouse/silver")
GOLD_PATH = os.getenv("GOLD_PATH", "/data/lakehouse/gold")

SYMBOLS = os.getenv(
    "STOCK_SYMBOLS", "AAPL,MSFT,GOOGL,AMZN,TSLA,META,NVDA,JPM,V,JNJ"
).split(",")

# Base prices for realistic mock data
BASE_PRICES: dict[str, float] = {
    "AAPL": 189.5, "MSFT": 415.0, "GOOGL": 175.2, "AMZN": 185.3,
    "TSLA": 245.8, "META": 505.0, "NVDA": 875.0, "JPM": 198.5,
    "V": 275.3, "JNJ": 158.2,
}


class LakehouseReader:
    """
    Reads from Delta Lake if available; falls back to mock data
    during development before Spark pipeline is running.
    """

    def _delta_available(self, path: str) -> bool:
        return os.path.isdir(path) and os.path.exists(
            os.path.join(path, "_delta_log")
        )

    # ─────────────────────────────────────────────────────
    # Quotes
    # ─────────────────────────────────────────────────────
    def get_latest_quotes(
        self,
        symbol: Optional[str] = None,
        limit: int = 50,
    ) -> list[dict]:
        silver_quotes = f"{SILVER_PATH}/stock_quotes"
        if self._delta_available(silver_quotes):
            return self._read_delta_quotes(symbol, limit)
        return self._mock_quotes(symbol)

    def _mock_quotes(self, symbol: Optional[str] = None) -> list[dict]:
        symbols = [symbol] if symbol else SYMBOLS
        quotes = []
        for sym in symbols:
            base = BASE_PRICES.get(sym, 100.0)
            price = round(base * (1 + random.uniform(-0.02, 0.02)), 2)
            prev = round(base * (1 + random.uniform(-0.01, 0.01)), 2)
            quotes.append({
                "symbol": sym,
                "price": price,
                "open": round(base * (1 + random.uniform(-0.01, 0.01)), 2),
                "high": round(price * 1.01, 2),
                "low": round(price * 0.99, 2),
                "prev_close": prev,
                "volume": random.randint(1_000_000, 50_000_000),
                "change": round(price - prev, 4),
                "change_pct": round((price - prev) / prev * 100, 4),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "provider": "mock",
            })
        return quotes

    def _read_delta_quotes(
        self, symbol: Optional[str], limit: int
    ) -> list[dict]:
        """Read latest records from Silver Delta table."""
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql import functions as F

            spark = SparkSession.getActiveSession()
            if spark is None:
                return self._mock_quotes(symbol)

            df = spark.read.format("delta").load(
                f"{SILVER_PATH}/stock_quotes"
            )
            if symbol:
                df = df.filter(F.col("symbol") == symbol)

            rows = (
                df.orderBy(F.col("silver_processed_at").desc())
                .limit(limit)
                .toPandas()
                .to_dict(orient="records")
            )
            return rows
        except Exception as exc:
            logger.warning("Delta read failed, using mock: %s", exc)
            return self._mock_quotes(symbol)

    # ─────────────────────────────────────────────────────
    # OHLCV History
    # ─────────────────────────────────────────────────────
    def get_ohlcv_history(
        self,
        symbol: str,
        hours: int = 24,
        interval: str = "1m",
    ) -> list[dict]:
        """Return candlestick history for a symbol."""
        gold_ohlcv = f"{GOLD_PATH}/ohlcv_1m"
        if self._delta_available(gold_ohlcv):
            return self._read_delta_ohlcv(symbol, hours)
        return self._mock_ohlcv(symbol, hours)

    def _mock_ohlcv(self, symbol: str, hours: int) -> list[dict]:
        base = BASE_PRICES.get(symbol, 100.0)
        candles = []
        now = datetime.now(timezone.utc)
        price = base
        for i in range(hours * 60, 0, -1):
            ts = now - timedelta(minutes=i)
            change = random.uniform(-0.003, 0.003)
            open_ = round(price, 2)
            close = round(price * (1 + change), 2)
            high = round(max(open_, close) * 1.002, 2)
            low = round(min(open_, close) * 0.998, 2)
            candles.append({
                "window_start": ts.isoformat(),
                "symbol": symbol,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "total_volume": random.randint(10_000, 500_000),
            })
            price = close
        return candles

    def _read_delta_ohlcv(self, symbol: str, hours: int) -> list[dict]:
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql import functions as F

            spark = SparkSession.getActiveSession()
            if not spark:
                return self._mock_ohlcv(symbol, hours)

            cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
            df = (
                spark.read.format("delta")
                .load(f"{GOLD_PATH}/ohlcv_1m")
                .filter(F.col("symbol") == symbol)
                .filter(F.col("window_start") >= cutoff.isoformat())
                .orderBy("window_start")
            )
            return df.toPandas().to_dict(orient="records")
        except Exception as exc:
            logger.warning("Delta OHLCV read failed: %s", exc)
            return self._mock_ohlcv(symbol, hours)

    # ─────────────────────────────────────────────────────
    # Top Movers
    # ─────────────────────────────────────────────────────
    def get_top_movers(self, direction: str, limit: int = 10) -> dict:
        quotes = self._mock_quotes()
        reverse = direction == "gainers"
        sorted_q = sorted(
            quotes, key=lambda x: x["change_pct"], reverse=reverse
        )[:limit]
        return {
            "direction": direction,
            "count": len(sorted_q),
            "data": sorted_q,
            "as_of": datetime.now(timezone.utc).isoformat(),
        }

    # ─────────────────────────────────────────────────────
    # Moving Averages
    # ─────────────────────────────────────────────────────
    def get_moving_averages(self, symbol: str, window: int = 5) -> dict:
        candles = self._mock_ohlcv(symbol, hours=2)
        prices = [c["close"] for c in candles]
        mas = []
        for i in range(window - 1, len(prices)):
            window_prices = prices[i - window + 1 : i + 1]
            mas.append({
                "index": i,
                "timestamp": candles[i]["window_start"],
                "price": prices[i],
                f"ma_{window}m": round(sum(window_prices) / len(window_prices), 4),
            })
        return {"symbol": symbol, "window_minutes": window, "data": mas[-50:]}

    # ─────────────────────────────────────────────────────
    # Volatility
    # ─────────────────────────────────────────────────────
    def get_volatility_metrics(
        self, symbol: Optional[str] = None, limit: int = 20
    ) -> dict:
        symbols = [symbol] if symbol else SYMBOLS[:limit]
        data = []
        for sym in symbols:
            base = BASE_PRICES.get(sym, 100.0)
            vol = round(random.uniform(0.5, 3.5), 4)
            data.append({
                "symbol": sym,
                "price_volatility": vol,
                "avg_range_pct": round(vol * 0.8, 4),
                "max_gain_pct": round(random.uniform(0, vol * 2), 4),
                "max_loss_pct": round(-random.uniform(0, vol * 2), 4),
                "bullish_pct": round(random.uniform(30, 70), 2),
                "as_of": datetime.now(timezone.utc).isoformat(),
            })
        return {"count": len(data), "data": data}

    # ─────────────────────────────────────────────────────
    # Market Summary
    # ─────────────────────────────────────────────────────
    def get_market_summary(self) -> dict:
        quotes = self._mock_quotes()
        gainers = sum(1 for q in quotes if q["change_pct"] > 0)
        return {
            "total_symbols": len(quotes),
            "gainers": gainers,
            "losers": len(quotes) - gainers,
            "avg_change_pct": round(
                sum(q["change_pct"] for q in quotes) / len(quotes), 4
            ),
            "total_volume": sum(q["volume"] for q in quotes),
            "as_of": datetime.now(timezone.utc).isoformat(),
        }

    # ─────────────────────────────────────────────────────
    # Trends
    # ─────────────────────────────────────────────────────
    def get_trends(
        self, hours: int = 6, symbol: Optional[str] = None
    ) -> dict:
        symbols = [symbol] if symbol else SYMBOLS
        trends = []
        for sym in symbols:
            candles = self._mock_ohlcv(sym, hours=hours)
            if len(candles) < 2:
                continue
            start_price = candles[0]["close"]
            end_price = candles[-1]["close"]
            chg = (end_price - start_price) / start_price * 100
            direction = (
                "uptrend" if chg > 1 else "downtrend" if chg < -1 else "sideways"
            )
            trends.append({
                "symbol": sym,
                "direction": direction,
                "change_over_period_pct": round(chg, 4),
                "start_price": start_price,
                "end_price": end_price,
                "period_hours": hours,
            })
        return {"count": len(trends), "data": trends}

    # ─────────────────────────────────────────────────────
    # Anomalies (simple Z-score based)
    # ─────────────────────────────────────────────────────
    def get_anomalies(
        self, hours: int = 1, symbol: Optional[str] = None
    ) -> dict:
        symbols = [symbol] if symbol else SYMBOLS
        anomalies = []
        for sym in symbols:
            candles = self._mock_ohlcv(sym, hours=hours)
            prices = [c["close"] for c in candles]
            if len(prices) < 10:
                continue
            avg = sum(prices) / len(prices)
            std = (sum((p - avg) ** 2 for p in prices) / len(prices)) ** 0.5
            for c in candles[-10:]:
                z = abs((c["close"] - avg) / std) if std > 0 else 0
                if z > 2.5:
                    anomalies.append({
                        "symbol": sym,
                        "timestamp": c["window_start"],
                        "price": c["close"],
                        "z_score": round(z, 4),
                        "type": "price_spike" if c["close"] > avg else "price_drop",
                    })
        return {
            "period_hours": hours,
            "count": len(anomalies),
            "data": anomalies,
        }

    # ─────────────────────────────────────────────────────
    # Pipeline Health
    # ─────────────────────────────────────────────────────
    def get_pipeline_health(self) -> dict:
        return {
            "bronze": self._delta_available(f"{SILVER_PATH}/../bronze/stock_events"),
            "silver": self._delta_available(f"{SILVER_PATH}/stock_quotes"),
            "gold_ohlcv": self._delta_available(f"{GOLD_PATH}/ohlcv_1m"),
            "gold_ma": self._delta_available(f"{GOLD_PATH}/moving_avg_5m"),
            "gold_vol": self._delta_available(f"{GOLD_PATH}/volatility"),
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
