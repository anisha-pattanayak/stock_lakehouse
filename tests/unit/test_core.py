"""
Unit Tests — Stock Ingestion & API Layer
Run: pytest tests/unit/ -v
"""

from __future__ import annotations

import sys
import os
import pytest

# Add source paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../ingestion"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../api"))


# ─────────────────────────────────────────────────────────
# Tests: StockQuote dataclass
# ─────────────────────────────────────────────────────────
from stock_client import StockQuote


class TestStockQuote:
    def test_change_calculated_correctly(self):
        quote = StockQuote(
            symbol="AAPL",
            price=190.0,
            open=188.0,
            high=192.0,
            low=187.5,
            prev_close=185.0,
            volume=50_000_000,
            timestamp="2024-01-15T15:30:00+00:00",
            provider="test",
        )
        assert quote.change == pytest.approx(5.0, abs=0.001)
        assert quote.change_pct == pytest.approx(2.7027, abs=0.001)

    def test_change_negative(self):
        quote = StockQuote(
            symbol="TSLA",
            price=240.0,
            open=250.0,
            high=252.0,
            low=238.0,
            prev_close=250.0,
            volume=30_000_000,
            timestamp="2024-01-15T15:30:00+00:00",
            provider="test",
        )
        assert quote.change == pytest.approx(-10.0, abs=0.001)
        assert quote.change_pct < 0

    def test_to_dict_has_all_fields(self):
        quote = StockQuote(
            symbol="MSFT",
            price=420.0,
            open=415.0,
            high=425.0,
            low=413.0,
            prev_close=410.0,
            volume=25_000_000,
            timestamp="2024-01-15",
            provider="test",
        )
        d = quote.to_dict()
        required_keys = [
            "symbol", "price", "open", "high", "low",
            "prev_close", "volume", "change", "change_pct",
            "timestamp", "provider", "ingested_at",
        ]
        for key in required_keys:
            assert key in d, f"Missing key: {key}"

    def test_zero_prev_close_no_division_error(self):
        quote = StockQuote(
            symbol="NEW",
            price=10.0,
            open=10.0,
            high=10.5,
            low=9.5,
            prev_close=0.0,
            volume=100_000,
            timestamp="2024-01-15",
            provider="test",
        )
        assert quote.change_pct == 0.0

    def test_symbol_preserved_in_dict(self):
        quote = StockQuote(
            symbol="GOOGL",
            price=175.0,
            open=173.0,
            high=177.0,
            low=172.0,
            prev_close=170.0,
            volume=10_000_000,
            timestamp="2024-01-15",
            provider="test",
        )
        assert quote.to_dict()["symbol"] == "GOOGL"


# ─────────────────────────────────────────────────────────
# Tests: LakehouseReader (mock mode)
# ─────────────────────────────────────────────────────────
from services.lakehouse_reader import LakehouseReader


class TestLakehouseReader:
    def setup_method(self):
        self.reader = LakehouseReader()

    def test_get_latest_quotes_returns_list(self):
        quotes = self.reader.get_latest_quotes()
        assert isinstance(quotes, list)
        assert len(quotes) > 0

    def test_quote_has_required_fields(self):
        quotes = self.reader.get_latest_quotes()
        required = ["symbol", "price", "change", "change_pct", "volume"]
        for q in quotes:
            for field in required:
                assert field in q, f"Missing field {field} in quote"

    def test_filter_by_symbol(self):
        quotes = self.reader.get_latest_quotes(symbol="AAPL")
        assert all(q["symbol"] == "AAPL" for q in quotes)

    def test_price_is_positive(self):
        quotes = self.reader.get_latest_quotes()
        for q in quotes:
            assert q["price"] > 0, f"Price should be positive for {q['symbol']}"

    def test_ohlcv_history_returns_candles(self):
        candles = self.reader.get_ohlcv_history("AAPL", hours=1)
        assert isinstance(candles, list)
        assert len(candles) > 0

    def test_ohlcv_high_gte_low(self):
        candles = self.reader.get_ohlcv_history("MSFT", hours=1)
        for c in candles:
            assert c["high"] >= c["low"], "High must be >= Low"

    def test_top_gainers(self):
        result = self.reader.get_top_movers("gainers", limit=5)
        assert "data" in result
        assert len(result["data"]) <= 5

    def test_top_losers(self):
        result = self.reader.get_top_movers("losers", limit=5)
        assert "data" in result

    def test_market_summary_structure(self):
        summary = self.reader.get_market_summary()
        assert "gainers" in summary
        assert "losers" in summary
        assert "total_symbols" in summary
        assert summary["gainers"] + summary["losers"] == summary["total_symbols"]

    def test_volatility_metrics(self):
        vol = self.reader.get_volatility_metrics()
        assert "data" in vol
        for item in vol["data"]:
            assert "symbol" in item
            assert "price_volatility" in item

    def test_anomaly_detection_returns_dict(self):
        result = self.reader.get_anomalies(hours=1)
        assert "data" in result
        assert "count" in result

    def test_trends_return_direction(self):
        trends = self.reader.get_trends(hours=6)
        assert "data" in trends
        valid_directions = {"uptrend", "downtrend", "sideways"}
        for t in trends["data"]:
            assert t["direction"] in valid_directions
