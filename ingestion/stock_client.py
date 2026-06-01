"""
Stock Market API Client
Supports: Alpha Vantage, Finnhub, Polygon.io, Yahoo Finance
"""

from __future__ import annotations

import os
import time
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
import requests

logger = logging.getLogger(__name__)


@dataclass
class StockQuote:
    """Normalized stock quote across all providers."""
    symbol: str
    price: float
    open: float
    high: float
    low: float
    prev_close: float
    volume: int
    timestamp: str
    provider: str
    change: float = field(init=False)
    change_pct: float = field(init=False)

    def __post_init__(self) -> None:
        self.change = round(self.price - self.prev_close, 4)
        self.change_pct = (
            round((self.change / self.prev_close) * 100, 4)
            if self.prev_close > 0 else 0.0
        )

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "price": self.price,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "prev_close": self.prev_close,
            "volume": self.volume,
            "change": self.change,
            "change_pct": self.change_pct,
            "timestamp": self.timestamp,
            "provider": self.provider,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }


class BaseStockClient(ABC):
    """Abstract base client for stock APIs."""

    MAX_RETRIES = 3
    RETRY_BACKOFF = 2.0  # seconds

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def get_quote(self, symbol: str) -> Optional[StockQuote]:
        """Fetch a quote with retry logic."""
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                return self._fetch_quote(symbol)
            except requests.exceptions.HTTPError as exc:
                # Bug fix: requests has no RateLimitError; check HTTP 429 manually
                if exc.response is not None and exc.response.status_code == 429:
                    logger.warning("Rate limit hit for %s, backing off 60s...", symbol)
                    time.sleep(60)
                else:
                    logger.error(
                        "HTTP error attempt %d/%d for %s: %s",
                        attempt, self.MAX_RETRIES, symbol, exc,
                    )
                    if attempt < self.MAX_RETRIES:
                        time.sleep(self.RETRY_BACKOFF * attempt)
            except requests.exceptions.RequestException as exc:
                logger.error(
                    "Attempt %d/%d failed for %s: %s",
                    attempt, self.MAX_RETRIES, symbol, exc,
                )
                if attempt < self.MAX_RETRIES:
                    time.sleep(self.RETRY_BACKOFF * attempt)
            except (KeyError, ValueError, TypeError) as exc:
                logger.error("Parse error for %s: %s", symbol, exc)
                return None
        return None

    @abstractmethod
    def _fetch_quote(self, symbol: str) -> StockQuote:
        """Provider-specific fetch implementation."""


# ─────────────────────────────────────────────────────────
# Alpha Vantage Client
# ─────────────────────────────────────────────────────────
class AlphaVantageClient(BaseStockClient):
    BASE_URL = "https://www.alphavantage.co/query"

    def _fetch_quote(self, symbol: str) -> StockQuote:
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": symbol,
            "apikey": self.api_key,
        }
        resp = self.session.get(self.BASE_URL, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json().get("Global Quote", {})
        if not data:
            raise ValueError(f"Empty quote from Alpha Vantage for {symbol}")
        return StockQuote(
            symbol=symbol,
            price=float(data["05. price"]),
            open=float(data["02. open"]),
            high=float(data["03. high"]),
            low=float(data["04. low"]),
            prev_close=float(data["08. previous close"]),
            volume=int(data["06. volume"]),
            timestamp=data["07. latest trading day"],
            provider="alpha_vantage",
        )


# ─────────────────────────────────────────────────────────
# Finnhub Client
# ─────────────────────────────────────────────────────────
class FinnhubClient(BaseStockClient):
    BASE_URL = "https://finnhub.io/api/v1"

    def _fetch_quote(self, symbol: str) -> StockQuote:
        params = {"symbol": symbol, "token": self.api_key}
        resp = self.session.get(
            f"{self.BASE_URL}/quote", params=params, timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("c", 0) == 0:
            raise ValueError(f"Invalid quote from Finnhub for {symbol}")
        return StockQuote(
            symbol=symbol,
            price=float(data["c"]),
            open=float(data["o"]),
            high=float(data["h"]),
            low=float(data["l"]),
            prev_close=float(data["pc"]),
            volume=int(data.get("v", 0)),
            timestamp=datetime.fromtimestamp(
                data["t"], tz=timezone.utc
            ).isoformat(),
            provider="finnhub",
        )


# ─────────────────────────────────────────────────────────
# Polygon.io Client
# ─────────────────────────────────────────────────────────
class PolygonClient(BaseStockClient):
    BASE_URL = "https://api.polygon.io/v2"

    def _fetch_quote(self, symbol: str) -> StockQuote:
        url = f"{self.BASE_URL}/last/trade/{symbol}"
        params = {"apiKey": self.api_key}
        resp = self.session.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json().get("results", {})

        # Also fetch snapshot for OHLCV
        snap_url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}"
        snap = self.session.get(snap_url, params=params, timeout=10).json()
        day = snap.get("ticker", {}).get("day", {})
        prev = snap.get("ticker", {}).get("prevDay", {})

        return StockQuote(
            symbol=symbol,
            price=float(data.get("p", day.get("c", 0))),
            open=float(day.get("o", 0)),
            high=float(day.get("h", 0)),
            low=float(day.get("l", 0)),
            prev_close=float(prev.get("c", 0)),
            volume=int(day.get("v", 0)),
            timestamp=datetime.now(timezone.utc).isoformat(),
            provider="polygon",
        )


# ─────────────────────────────────────────────────────────
# Yahoo Finance (no key required — good for dev/testing)
# ─────────────────────────────────────────────────────────
class YahooFinanceClient(BaseStockClient):
    BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"

    def __init__(self) -> None:
        super().__init__(api_key="")
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; StockBot/1.0)"
        })

    def _fetch_quote(self, symbol: str) -> StockQuote:
        url = f"{self.BASE_URL}/{symbol}"
        params = {"interval": "1m", "range": "1d"}
        resp = self.session.get(url, params=params, timeout=10)
        resp.raise_for_status()
        meta = resp.json()["chart"]["result"][0]["meta"]
        return StockQuote(
            symbol=symbol,
            price=float(meta["regularMarketPrice"]),
            open=float(meta.get("regularMarketOpen", meta["regularMarketPrice"])),
            high=float(meta.get("regularMarketDayHigh", meta["regularMarketPrice"])),
            low=float(meta.get("regularMarketDayLow", meta["regularMarketPrice"])),
            prev_close=float(meta.get("previousClose", meta["regularMarketPrice"])),
            volume=int(meta.get("regularMarketVolume", 0)),
            timestamp=datetime.fromtimestamp(
                meta["regularMarketTime"], tz=timezone.utc
            ).isoformat(),
            provider="yahoo_finance",
        )


# ─────────────────────────────────────────────────────────
# Factory
# ─────────────────────────────────────────────────────────
def get_stock_client(provider: str = None) -> BaseStockClient:
    """Return the configured stock client."""
    provider = provider or os.getenv("API_PROVIDER", "yahoo_finance")
    match provider:
        case "alpha_vantage":
            key = os.environ["ALPHA_VANTAGE_API_KEY"]
            return AlphaVantageClient(key)
        case "finnhub":
            key = os.environ["FINNHUB_API_KEY"]
            return FinnhubClient(key)
        case "polygon":
            key = os.environ["POLYGON_API_KEY"]
            return PolygonClient(key)
        case "yahoo_finance" | _:
            return YahooFinanceClient()
