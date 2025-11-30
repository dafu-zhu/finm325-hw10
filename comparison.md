# SQLite3 vs Parquet Format Comparison

## Executive Summary

This document compares **SQLite3** (relational database) and **Parquet** (columnar file format) for storing and querying multi-ticker OHLCV market data. Based on performance benchmarks with 9,775 records across 5 tickers, we evaluate storage efficiency, query performance, and use case suitability for financial trading systems.

---

## Performance Comparison

### 1. File Size

| Format  | Size (KB) | Size (Bytes) | Compression Ratio |
|---------|-----------|--------------|-------------------|
| SQLite3 | 672 KB    | 688,128      | Baseline          |
| Parquet | 342 KB    | 350,207      | **49% smaller**   |

**Winner:** Parquet

**Analysis:**
- Parquet achieves nearly 50% storage reduction through:
  - Columnar compression (similar values grouped together)
  - Efficient encoding schemes (dictionary encoding, RLE)
  - Partitioning by ticker reduces metadata overhead
- SQLite3 stores row-oriented data with indexes and metadata overhead

---

### 2. Query Speed

#### Simple Range Query (Retrieve ticker data by date range)

| Format  | Query Time (seconds) | Speedup        |
|---------|---------------------|----------------|
| SQLite3 | 0.0031              | Baseline       |
| Parquet | 0.0038              | 0.82x (slower) |

**Winner:** SQLite3 (marginally faster)

**Analysis:**
- SQLite3 benefits from indexes on timestamp and ticker_id
- Parquet requires scanning partition files
- For small datasets, performance difference is negligible (< 1ms)

---

#### Aggregation Query (Average daily volume)

| Format  | Query Time (seconds) | Query Type         |
|---------|---------------------|--------------------|
| SQLite3 | 0.0052              | GROUP BY + JOIN    |

**Winner:** SQLite3 (purpose-built for this)

**Analysis:**
- SQLite3 excels at relational aggregations with GROUP BY
- Parquet would require custom aggregation logic in pandas
- SQL provides declarative, optimized query execution

---

#### Analytical Query (Rolling volatility computation)

| Format  | Query Time (seconds) | Data Scanned     |
|---------|---------------------|------------------|
| Parquet | 0.0202              | All rows         |

**Winner:** Parquet (optimized for columnar analytics)

**Analysis:**
- Parquet efficiently scans only required columns (close price)
- Columnar layout perfect for time-series operations
- pandas integration provides powerful analytics functions
- SQLite3 would require window functions or self-joins

---

### 3. Ease of Integration

#### SQLite3
**Pros:**
- Standard SQL interface (universal compatibility)
- Built-in Python support (`sqlite3` module)
- ACID compliance ensures data integrity
- Concurrent reads with write locking
- Rich ecosystem of SQL tools

**Cons:**
- Requires schema design upfront
- Limited scalability for very large datasets
- Row-oriented storage less efficient for analytics
- Window functions can be complex

#### Parquet
**Pros:**
- Seamless pandas integration
- Excellent for big data ecosystems (Spark, Dask)
- Column pruning reduces I/O
- Partition pushdown filtering
- Schema evolution support

**Cons:**
- No built-in query language (requires pandas/SQL layer)
- No transactional guarantees
- File-based (not a database)
- Limited concurrent write support

---

## Use Case Analysis

### When to Use SQLite3

#### 1. **Live Trading Systems**
- **Why:** ACID compliance ensures no data loss during crashes
- **Example:** Recording trade executions, order updates, position changes
- **Benefits:**
  - Atomic writes prevent partial updates
  - Transactional consistency for multi-table operations
  - Concurrent read access for multiple strategies

#### 2. **Complex Relational Queries**
- **Why:** SQL excels at joins, aggregations, and filtering
- **Example:**
  - "Find all trades where ticker X crossed moving average Y"
  - "Calculate portfolio P&L across multiple accounts"
- **Benefits:**
  - Declarative query language
  - Query optimizer handles execution plan
  - Indexes accelerate lookups

#### 3. **Small to Medium Datasets**
- **Why:** Excellent performance for datasets < 100GB
- **Example:** Intraday tick data for a few symbols
- **Benefits:**
  - Single-file portability
  - No external dependencies
  - Fast local queries

#### 4. **Multi-User Applications**
- **Why:** Built-in concurrency control
- **Example:** Trading dashboard with multiple viewers
- **Benefits:**
  - Read locks don't block other reads
  - Write locks ensure consistency

---

### When to Use Parquet

#### 1. **Historical Data Storage**
- **Why:** Storage efficiency critical for years of tick data
- **Example:** Storing 10+ years of OHLCV data for backtesting
- **Benefits:**
  - 50% storage savings
  - Cheap cloud storage (S3, GCS)
  - Easy archival and retrieval

#### 2. **Analytical Workloads**
- **Why:** Columnar format optimized for scanning
- **Example:**
  - Computing rolling statistics (volatility, beta, correlation)
  - Feature engineering for ML models
  - Time-series analysis
- **Benefits:**
  - Read only required columns (10x faster for wide tables)
  - Efficient compression on numeric data
  - Native pandas/NumPy integration

#### 3. **Big Data Processing**
- **Why:** Integration with distributed computing frameworks
- **Example:** Processing terabytes of market data
- **Benefits:**
  - Spark/Dask can read Parquet in parallel
  - Partition pruning skips irrelevant files
  - Predicate pushdown reduces data transfer

#### 4. **Research and Backtesting**
- **Why:** Immutable historical data, read-heavy workloads
- **Example:** Running 1000 backtest simulations
- **Benefits:**
  - No database server overhead
  - Version data files in git/S3
  - Reproducible research (data + code)

---

## Hybrid Architecture Recommendations

### Recommended Setup for Trading Systems

```
┌─────────────────────────────────────────────────────┐
│                  Trading System                     │
├─────────────────────────────────────────────────────┤
│                                                     │
│  ┌─────────────────┐        ┌──────────────────┐    │
│  │   Live Trading  │        │   Backtesting    │    │
│  │                 │        │   & Research     │    │
│  │   SQLite3 DB    │───────▶│   Parquet Files  │    │
│  │                 │  ETL   │                  │    │
│  │  - Orders       │        │  - Historical    │    │
│  │  - Executions   │        │  - Features      │    │
│  │  - Positions    │        │  - Simulations   │    │
│  └─────────────────┘        └──────────────────┘    │
│         │                            │              │
│         │                            │              │
│   ┌─────▼────────────────────────────▼──────────┐   │
│   │         Analytics Pipeline                  │   │
│   │  (pandas, NumPy, scikit-learn)              │   │
│   └─────────────────────────────────────────────┘   │
│                                                     │
└─────────────────────────────────────────────────────┘
```

#### Workflow:
1. **Intraday:** Write live data to SQLite3 for ACID compliance
2. **End-of-Day:** Export to Parquet for archival and analytics
3. **Backtesting:** Load historical Parquet files
4. **Research:** Query Parquet for feature engineering
5. **Monitoring:** Real-time queries on SQLite3

---

## Detailed Use Case Scenarios

### Scenario 1: Intraday Algo Trading
**Best Choice:** SQLite3

**Rationale:**
- Need to record every order/execution with ACID guarantees
- Frequent writes (thousands per second)
- Complex joins (orders → executions → positions)
- Real-time risk calculations require current state

**Example:**
```sql
-- Get current position for risk check
SELECT SUM(quantity * side) as net_position
FROM executions
WHERE ticker = 'AAPL' AND strategy = 'momentum'
```

---

### Scenario 2: End-of-Day Analytics
**Best Choice:** Parquet

**Rationale:**
- Compute statistics across all tickers
- Read-only workload (no concurrent writes)
- Scan entire dataset for patterns
- Storage efficiency for long history

**Example:**
```python
# Compute correlation matrix across all tickers
df = pd.read_parquet('market_data/')
returns = df.groupby('ticker')['close'].pct_change()
correlation = returns.unstack().corr()
```

---

### Scenario 3: Backtesting Framework
**Best Choice:** Parquet

**Rationale:**
- Reproducible research requires versioned data
- Parallel backtests on different time periods
- Fast column access (only need OHLC, not volume)
- Integration with ML pipelines

**Example:**
```python
# Load 5 years of data for backtesting
df = pd.read_parquet(
    'market_data/',
    filters=[('timestamp', '>=', '2019-01-01')]
)
```

---

### Scenario 4: Real-Time Dashboard
**Best Choice:** SQLite3

**Rationale:**
- Multiple users querying simultaneously
- Need latest data (< 1 second old)
- Complex aggregations (P&L by strategy, sector)
- ACID ensures consistent snapshots

**Example:**
```sql
-- Dashboard query: Top performers today
SELECT ticker,
       ((last_price - first_price) / first_price * 100) as return
FROM daily_summary
WHERE trade_date = DATE('now')
ORDER BY return DESC
LIMIT 10
```

---

## Performance Scaling

### Dataset Size Projections

| Dataset Size | SQLite3 Performance | Parquet Performance | Recommendation |
|--------------|---------------------|---------------------|----------------|
| < 1 GB       | Excellent           | Excellent           | Either         |
| 1-10 GB      | Good                | Excellent           | Either         |
| 10-100 GB    | Moderate            | Excellent           | Parquet        |
| 100+ GB      | Poor                | Excellent           | Parquet + Dask |

**Note:** SQLite3 performance degrades with large datasets due to:
- Index size growing beyond memory
- Full table scans on analytical queries
- Limited parallelization

---

## Conclusion

### Summary Table

| Criteria                | SQLite3        | Parquet       | Winner   |
|-------------------------|----------------|---------------|----------|
| Storage Size            | 672 KB         | 342 KB        | Parquet  |
| Simple Query Speed      | 0.0031s        | 0.0038s       | SQLite3  |
| Analytical Query Speed  | Complex SQL    | 0.02s native  | Parquet  |
| ACID Compliance         | Yes            | No            | SQLite3  |
| Concurrent Writes       | Limited        | No            | SQLite3  |
| Concurrent Reads        | Yes            | Yes           | Tie      |
| Compression             | Limited        | Excellent     | Parquet  |
| Schema Flexibility      | Fixed schema   | Schema evolve | Parquet  |
| Query Language          | SQL (powerful) | pandas/SQL    | SQLite3  |
| Big Data Integration    | Limited        | Excellent     | Parquet  |

### Final Recommendation

**Use Both in a Hybrid Architecture:**

1. **SQLite3** for operational data:
   - Live trading state
   - Transactional integrity
   - Real-time queries

2. **Parquet** for analytical data:
   - Historical archives
   - Backtesting datasets
   - ML feature stores

3. **ETL Pipeline:**
   - Hourly/Daily: SQLite3 → Parquet
   - Compress and partition by date/ticker
   - Maintain last N days in SQLite3 for fast access

This approach leverages the strengths of each format while mitigating their weaknesses, providing an optimal solution for financial data systems.
