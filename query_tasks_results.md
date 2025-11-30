# Query Tasks - Results

## SQLite3 Queries

### Query 1: Retrieve all data for TSLA between 2025-11-17 and 2025-11-18

**Execution Time:** 0.0031 seconds

**Query:**
```sql
SELECT
    p.timestamp,
    t.symbol,
    p.open,
    p.high,
    p.low,
    p.close,
    p.volume
FROM prices p
JOIN tickers t ON p.ticker_id = t.ticker_id
WHERE t.symbol = 'TSLA'
  AND p.timestamp >= '2025-11-17'
  AND p.timestamp <= '2025-11-18 23:59:59'
ORDER BY p.timestamp
```

**Results (first 10 rows):**
| timestamp           | symbol | open   | high   | low    | close  | volume |
|---------------------|--------|--------|--------|--------|--------|--------|
| 2025-11-17 09:30:00 | TSLA   | 268.31 | 268.51 | 267.95 | 268.07 | 1609   |
| 2025-11-17 09:31:00 | TSLA   | 268.94 | 269.11 | 268.28 | 269.04 | 4809   |
| 2025-11-17 09:32:00 | TSLA   | 267.70 | 267.94 | 267.69 | 267.92 | 1997   |
| 2025-11-17 09:33:00 | TSLA   | 268.45 | 268.64 | 268.00 | 268.56 | 3461   |
| 2025-11-17 09:34:00 | TSLA   | 269.01 | 269.57 | 268.21 | 269.23 | 4003   |

**Total Rows:** 782

---

### Query 2: Calculate average daily volume per ticker

**Execution Time:** 0.0052 seconds

**Query:**
```sql
SELECT
    t.symbol,
    AVG(daily_volume) as avg_daily_volume
FROM (
    SELECT
        p.ticker_id,
        DATE(p.timestamp) as trade_date,
        SUM(p.volume) as daily_volume
    FROM prices p
    GROUP BY p.ticker_id, DATE(p.timestamp)
) daily
JOIN tickers t ON daily.ticker_id = t.ticker_id
GROUP BY t.symbol
ORDER BY avg_daily_volume DESC
```

**Results:**
| symbol | avg_daily_volume |
|--------|------------------|
| TSLA   | 1,085,973.0      |
| AAPL   | 1,082,222.6      |
| AMZN   | 1,076,588.8      |
| GOOG   | 1,071,402.8      |
| MSFT   | 1,050,441.4      |

---

### Query 3: Identify top 3 tickers by return over the full period

**Execution Time:** 0.0110 seconds

**Query:**
```sql
SELECT
    t.symbol,
    p1.close as first_price,
    p2.close as last_price,
    ((p2.close - p1.close) / p1.close * 100) as return_pct
FROM (
    SELECT
        ticker_id,
        MIN(timestamp) as first_time,
        MAX(timestamp) as last_time
    FROM prices p
    GROUP BY ticker_id
) times
JOIN tickers t ON times.ticker_id = t.ticker_id
JOIN prices p1 ON times.ticker_id = p1.ticker_id AND times.first_time = p1.timestamp
JOIN prices p2 ON times.ticker_id = p2.ticker_id AND times.last_time = p2.timestamp
ORDER BY return_pct DESC
LIMIT 3
```

**Results:**
| symbol | first_price | last_price | return_pct |
|--------|-------------|------------|------------|
| MSFT   | 183.89      | 245.70     | 33.61%     |
| AAPL   | 270.88      | 334.57     | 23.51%     |
| GOOG   | 139.43      | 153.90     | 10.38%     |

---

### Query 4: Find first and last trade price for each ticker per day

**Execution Time:** 0.0228 seconds

**Query:**
```sql
SELECT
    t.symbol,
    first_times.trade_date,
    first_prices.close as first_price,
    first_times.first_time,
    last_prices.close as last_price,
    last_times.last_time
FROM tickers t
JOIN (
    SELECT
        ticker_id,
        DATE(timestamp) as trade_date,
        MIN(timestamp) as first_time
    FROM prices
    GROUP BY ticker_id, DATE(timestamp)
) first_times ON t.ticker_id = first_times.ticker_id
JOIN prices first_prices
    ON first_times.ticker_id = first_prices.ticker_id
    AND first_times.first_time = first_prices.timestamp
JOIN (
    SELECT
        ticker_id,
        DATE(timestamp) as trade_date,
        MAX(timestamp) as last_time
    FROM prices
    GROUP BY ticker_id, DATE(timestamp)
) last_times
    ON t.ticker_id = last_times.ticker_id
    AND first_times.trade_date = last_times.trade_date
JOIN prices last_prices
    ON last_times.ticker_id = last_prices.ticker_id
    AND last_times.last_time = last_prices.timestamp
ORDER BY first_times.trade_date, t.symbol
```

**Results (first 10 rows):**
| symbol | trade_date | first_price | first_time          | last_price | last_time           |
|--------|------------|-------------|---------------------|------------|---------------------|
| AAPL   | 2025-11-17 | 270.88      | 2025-11-17 09:30:00 | 287.68     | 2025-11-17 16:00:00 |
| AMZN   | 2025-11-17 | 125.46      | 2025-11-17 09:30:00 | 141.03     | 2025-11-17 16:00:00 |
| GOOG   | 2025-11-17 | 139.43      | 2025-11-17 09:30:00 | 105.00     | 2025-11-17 16:00:00 |
| MSFT   | 2025-11-17 | 183.89      | 2025-11-17 09:30:00 | 215.36     | 2025-11-17 16:00:00 |
| TSLA   | 2025-11-17 | 268.07      | 2025-11-17 09:30:00 | 286.86     | 2025-11-17 16:00:00 |

**Total Rows:** 25 (5 tickers × 5 days)

---

## Parquet Queries

### Query 1: Load all data for AAPL and compute 5-period rolling average of close price

**Execution Time:** 0.0103 seconds

**Method:**
```python
result = storage.compute_rolling_average('AAPL', window=5, column='close')
```

**Results (first 10 rows):**
| timestamp           | close  | close_rolling_5 |
|---------------------|--------|-----------------|
| 2025-11-17 09:30:00 | 270.88 | NaN             |
| 2025-11-17 09:31:00 | 269.24 | NaN             |
| 2025-11-17 09:32:00 | 270.86 | NaN             |
| 2025-11-17 09:33:00 | 269.28 | NaN             |
| 2025-11-17 09:34:00 | 269.32 | 269.916         |
| 2025-11-17 09:35:00 | 270.23 | 269.786         |
| 2025-11-17 09:36:00 | 270.45 | 270.028         |
| 2025-11-17 09:37:00 | 269.52 | 269.760         |
| 2025-11-17 09:38:00 | 270.72 | 270.048         |
| 2025-11-17 09:39:00 | 270.70 | 270.324         |

**Total Rows:** 1,955

---

### Query 2: Compute 5-period rolling volatility (std dev) of returns for each ticker

**Execution Time:** 0.0202 seconds

**Method:**
```python
result = storage.compute_rolling_volatility(window=5)
```

**Results (sample by ticker - first 5 rows per ticker):**
| timestamp           | ticker | close  | return     | rolling_volatility |
|---------------------|--------|--------|------------|--------------------|
| 2025-11-17 09:30:00 | AAPL   | 270.88 | NaN        | NaN                |
| 2025-11-17 09:31:00 | AAPL   | 269.24 | -0.006054  | NaN                |
| 2025-11-17 09:32:00 | AAPL   | 270.86 | 0.006017   | NaN                |
| 2025-11-17 09:33:00 | AAPL   | 269.28 | -0.005833  | NaN                |
| 2025-11-17 09:34:00 | AAPL   | 269.32 | 0.000149   | NaN                |
| 2025-11-17 09:35:00 | AAPL   | 270.23 | 0.003379   | 0.005414           |

**Total Rows:** 9,775 (all tickers, all timestamps)

---

### Query 3: Compare query time and file size with SQLite3 for Task 1

**Retrieve TSLA data for 2025-11-17 to 2025-11-18**

**Execution Time:** 0.0038 seconds

**Results (first 10 rows):**
| timestamp           | ticker | open   | high   | low    | close  | volume |
|---------------------|--------|--------|--------|--------|--------|--------|
| 2025-11-17 09:30:00 | TSLA   | 268.31 | 268.51 | 267.95 | 268.07 | 1609   |
| 2025-11-17 09:31:00 | TSLA   | 268.94 | 269.11 | 268.28 | 269.04 | 4809   |
| 2025-11-17 09:32:00 | TSLA   | 267.70 | 267.94 | 267.69 | 267.92 | 1997   |
| 2025-11-17 09:33:00 | TSLA   | 268.45 | 268.64 | 268.00 | 268.56 | 3461   |
| 2025-11-17 09:34:00 | TSLA   | 269.01 | 269.57 | 268.21 | 269.23 | 4003   |

**Total Rows:** 782

**Performance Comparison:**
| Format  | Query Time | File Size |
|---------|-----------|-----------|
| SQLite3 | 0.0031s   | 672 KB    |
| Parquet | 0.0038s   | 342 KB    |

---

## Summary

### Key Findings

1. **Storage Efficiency**: Parquet is ~50% smaller (342 KB vs 672 KB)
2. **Query Performance**: Similar performance for simple queries (both < 0.01s)
3. **Analytical Queries**: Parquet excels at columnar operations (rolling averages, volatility)
4. **Relational Queries**: SQLite3 excels at complex joins and aggregations
