# API Documentation

Base URL: `http://localhost:8000`
Interactive docs: `http://localhost:8000/docs`

## Endpoints

### Stocks
| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/stocks/` | All live quotes |
| GET | `/api/stocks/{symbol}` | Single symbol quote |
| GET | `/api/stocks/{symbol}/history` | OHLCV history (hours, interval params) |
| GET | `/api/stocks/gainers/top` | Top gaining stocks |
| GET | `/api/stocks/losers/top` | Top losing stocks |

### Analytics
| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/analytics/moving-averages/{symbol}` | Moving averages |
| GET | `/api/analytics/volatility` | All symbols volatility |
| GET | `/api/analytics/volatility/{symbol}` | Single symbol volatility |
| GET | `/api/analytics/summary` | Market-wide summary |

### Other
| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/trends/` | Trend direction analysis |
| GET | `/api/health/` | System health |
| GET | `/api/health/pipeline` | Pipeline layer health |
| GET | `/api/anomalies/` | Detected anomalies |
