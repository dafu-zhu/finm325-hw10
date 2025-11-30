# HW10: Market Data Storage and Querying with SQLite3 and Parquet

A Python-based system for ingesting, storing, and querying multi-ticker market data using both **SQLite3** (relational database) and **Parquet** (columnar file format). This project demonstrates the tradeoffs between relational and columnar storage formats for financial analytics and trading workflows.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Usage](#usage)
- [Module Descriptions](#module-descriptions)
- [Running Tests](#running-tests)
- [Performance Results](#performance-results)
- [Key Findings](#key-findings)

---

## Overview

This assignment implements a complete data engineering pipeline for financial market data:

1. **Data Ingestion**: Load and validate multi-ticker OHLCV data from CSV
2. **SQLite3 Storage**: Store data in a normalized relational database
3. **Parquet Storage**: Store data in partitioned columnar format
4. **Querying**: Execute analytical queries on both formats
5. **Comparison**: Benchmark performance and storage efficiency

### Dataset

- **File**: `market_data_multi.csv`
- **Records**: 9,775 OHLCV bars
- **Tickers**: 5 (AAPL, MSFT, GOOG, TSLA, AMZN)
- **Period**: 5 days of minute-level data
- **Columns**: timestamp, ticker, open, high, low, close, volume

---

## Features

- Data validation (missing values, ticker consistency, price integrity)
- Normalized SQLite3 schema with foreign keys
- Partitioned Parquet storage by ticker
- Complex SQL queries (aggregations, joins, window functions)
- Pandas-based analytical queries (rolling statistics, volatility)
- Comprehensive unit tests
- Performance benchmarking
- Format comparison analysis

---

## Project Structure

```
hw10/
├── files/                          # Input data files
│   ├── market_data_multi.csv       # Multi-ticker OHLCV data
│   ├── tickers.csv                 # Ticker reference data
│   ├── schema.sql                  # SQLite database schema
│   └── query_tasks.md              # Query requirements
├── market_data.db                  # Generated SQLite database (672 KB)
├── market_data/                    # Generated Parquet directory (342 KB)
│   ├── ticker=AAPL/
│   ├── ticker=AMZN/
│   ├── ticker=GOOG/
│   ├── ticker=MSFT/
│   └── ticker=TSLA/
├── data_loader.py                  # Data ingestion and validation
├── sqlite_storage.py               # SQLite operations and queries
├── parquet_storage.py              # Parquet operations and queries
├── tests/                          # Unit tests
│   ├── test_data_loader.py
│   ├── test_sqlite_storage.py
│   └── test_parquet_storage.py
├── query_tasks_results.md          # Query results and benchmarks
├── comparison.md                   # Format comparison analysis
└── README.md                       # This file
```

---

## Installation

### Prerequisites

- Python 3.8+
- pip or uv package manager

### Dependencies

```bash
pip install pandas pyarrow pytest
```

Or using the project's pyproject.toml:

```bash
# If using uv
uv pip install -e .

# If using pip
pip install -e .
```

### Required Packages

- **pandas**: Data manipulation and CSV loading
- **pyarrow**: Parquet file reading/writing
- **pytest**: Unit testing framework
- **sqlite3**: Built-in Python module (no install needed)

---

## Usage

### 1. Data Loading and Validation

```python
from data_loader import DataLoader

loader = DataLoader()
market_df, tickers_df = loader.load_and_validate()

print(f"Loaded {len(market_df)} records for {len(tickers_df)} tickers")
```

**Output:**
```
✓ Data loaded and validated successfully!
Tickers loaded: 5
Market data shape: (9775, 7)
```

---

### 2. SQLite3 Storage and Querying

```python
from sqlite_storage import SQLiteStorage

# Initialize storage
storage = SQLiteStorage()

# Create schema and insert data
storage.create_schema()
storage.insert_tickers(tickers_df)
storage.insert_market_data(market_df, tickers_df)

# Query 1: Get ticker data by date range
tsla_data = storage.query_ticker_data_by_date_range(
    'TSLA', '2025-11-17', '2025-11-18 23:59:59'
)

# Query 2: Average daily volume
avg_volume = storage.query_average_daily_volume()

# Query 3: Top 3 tickers by return
top_tickers = storage.query_top_tickers_by_return(top_n=3)

# Query 4: Daily first and last prices
daily_prices = storage.query_daily_first_last_prices()

storage.close()
```

**Run the demo:**
```bash
cd src/finm_python/hw10
python sqlite_storage.py
```

---

### 3. Parquet Storage and Querying

```python
from parquet_storage import ParquetStorage

# Initialize storage
storage = ParquetStorage()

# Write partitioned data
storage.write_partitioned_data(market_df, tickers_df)

# Query 1: Rolling average
aapl_rolling = storage.compute_rolling_average('AAPL', window=5, column='close')

# Query 2: Rolling volatility
volatility = storage.compute_rolling_volatility(window=5)

# Query 3: Date range query
tsla_data = storage.query_ticker_data_by_date_range(
    'TSLA', '2025-11-17', '2025-11-18 23:59:59'
)
```

**Run the demo:**
```bash
cd src/finm_python/hw10
python parquet_storage.py
```

---

## Module Descriptions

### 1. `data_loader.py`

**Purpose**: Load and validate multi-ticker market data from CSV files.

**Key Classes:**
- `DataLoader`: Handles data ingestion and validation

**Key Methods:**
- `load_tickers()`: Load ticker reference data
- `load_market_data()`: Load and normalize OHLCV data
- `validate_data()`: Check for missing values and data integrity
- `load_and_validate()`: Combined load and validation

**Validations Performed:**
- No missing timestamps or prices
- All expected tickers are present
- Price integrity (high ≥ low)
- Consistent datetime formatting

---

### 2. `sqlite_storage.py`

**Purpose**: Store and query market data using SQLite3 relational database.

**Key Classes:**
- `SQLiteStorage`: Manages SQLite database operations

**Database Schema:**
```sql
-- Tickers table
CREATE TABLE tickers (
    ticker_id INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL UNIQUE,
    name TEXT,
    exchange TEXT
);

-- Prices table
CREATE TABLE prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    ticker_id INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id)
);
```

**Query Methods:**
1. `query_ticker_data_by_date_range()`: Retrieve data for specific ticker and date range
2. `query_average_daily_volume()`: Calculate average daily volume per ticker
3. `query_top_tickers_by_return()`: Identify top N tickers by return
4. `query_daily_first_last_prices()`: Get first and last prices per ticker per day

---

### 3. `parquet_storage.py`

**Purpose**: Store and query market data using Parquet columnar format.

**Key Classes:**
- `ParquetStorage`: Manages Parquet file operations

**Features:**
- Partitioned by ticker symbol
- Snappy compression
- Dictionary encoding
- Column pruning support

**Query Methods:**
1. `query_ticker_data_by_date_range()`: Retrieve data for specific ticker
2. `compute_rolling_average()`: Calculate rolling average for a column
3. `compute_rolling_volatility()`: Calculate rolling volatility of returns
4. `read_all_data()`: Load entire dataset
5. `get_partition_info()`: Get partition statistics

---

## Running Tests

### Run All Tests

```bash
cd src/finm_python/hw10
pytest tests/ -v
```

### Run Specific Test Modules

```bash
# Test data loader
pytest tests/test_data_loader.py -v

# Test SQLite storage
pytest tests/test_sqlite_storage.py -v

# Test Parquet storage
pytest tests/test_parquet_storage.py -v
```

### Test Coverage

The test suite includes:
- **15+ unit tests** for data loading and validation
- **10+ unit tests** for SQLite operations and queries
- **10+ unit tests** for Parquet operations and queries

**Example output:**
```
tests/test_data_loader.py::TestDataLoader::test_load_tickers PASSED
tests/test_data_loader.py::TestDataLoader::test_validate_data_success PASSED
tests/test_sqlite_storage.py::TestSQLiteStorage::test_create_schema PASSED
tests/test_parquet_storage.py::TestParquetStorage::test_write_partitioned_data PASSED
```

---

## Performance Results

### Storage Comparison

| Format  | Size     | Compression vs Raw |
|---------|----------|--------------------|
| SQLite3 | 672 KB   | Baseline           |
| Parquet | 342 KB   | **49% smaller**    |

### Query Performance

| Query                        | SQLite3  | Parquet  | Winner   |
|------------------------------|----------|----------|----------|
| Ticker data by date range    | 0.0031s  | 0.0038s  | SQLite3  |
| Average daily volume         | 0.0052s  | N/A      | SQLite3  |
| Top tickers by return        | 0.0110s  | N/A      | SQLite3  |
| Daily first/last prices      | 0.0228s  | N/A      | SQLite3  |
| Rolling average (5-period)   | N/A      | 0.0103s  | Parquet  |
| Rolling volatility (5-period)| N/A      | 0.0202s  | Parquet  |

**Key Takeaways:**
- **Parquet**: 50% storage savings, excellent for columnar analytics
- **SQLite3**: Faster for relational queries, better for transactional workloads
- Both formats perform well for small-medium datasets (< 10K records)

See [query_tasks_results.md](query_tasks_results.md) for detailed results.

---

## Key Findings

### When to Use SQLite3

1. **Transactional Systems**: ACID compliance for live trading
2. **Relational Queries**: Complex joins and aggregations
3. **Multi-User Access**: Concurrent reads with write locking
4. **Small-Medium Data**: Excellent performance for < 100GB datasets

### When to Use Parquet

1. **Historical Archives**: 50% storage savings for long-term data
2. **Analytical Workloads**: Columnar access for statistics and ML
3. **Big Data Integration**: Works with Spark, Dask, Arrow
4. **Read-Heavy Workflows**: Backtesting and research

### Recommended Hybrid Architecture

```
Live Trading (SQLite3) → ETL Pipeline → Historical Archive (Parquet)
                                              ↓
                                      Backtesting & Research
```

See [comparison.md](comparison.md) for detailed analysis.

---

## Example Queries

### SQLite3: Top Performers

```sql
SELECT
    t.symbol,
    p1.close as first_price,
    p2.close as last_price,
    ((p2.close - p1.close) / p1.close * 100) as return_pct
FROM (
    SELECT ticker_id,
           MIN(timestamp) as first_time,
           MAX(timestamp) as last_time
    FROM prices
    GROUP BY ticker_id
) times
JOIN tickers t ON times.ticker_id = t.ticker_id
JOIN prices p1 ON times.ticker_id = p1.ticker_id
    AND times.first_time = p1.timestamp
JOIN prices p2 ON times.ticker_id = p2.ticker_id
    AND times.last_time = p2.timestamp
ORDER BY return_pct DESC
LIMIT 3;
```

### Parquet: Rolling Volatility

```python
import pandas as pd

# Load all data
df = pd.read_parquet('market_data/')

# Calculate returns
df['return'] = df.groupby('ticker')['close'].pct_change()

# Calculate rolling volatility
df['volatility'] = df.groupby('ticker')['return'].transform(
    lambda x: x.rolling(window=5).std()
)
```