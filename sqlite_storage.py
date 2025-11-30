"""
SQLite3 storage module for multi-ticker market data.

This module provides functionality to create a normalized SQLite database schema,
insert market data, and execute various analytical queries.
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional, Tuple
import time


class SQLiteStorage:
    """Manages SQLite3 storage and querying for market data."""

    def __init__(self, db_path: Path = None, schema_path: Path = None):
        """
        Initialize SQLite storage.

        Args:
            db_path: Path to the SQLite database file.
                    Defaults to 'market_data.db' in the hw10 directory.
            schema_path: Path to the schema SQL file.
                        Defaults to 'files/schema.sql'.
        """
        if db_path is None:
            db_path = Path(__file__).parent / "market_data.db"
        if schema_path is None:
            schema_path = Path(__file__).parent / "files" / "schema.sql"

        self.db_path = Path(db_path)
        self.schema_path = Path(schema_path)
        self.conn: Optional[sqlite3.Connection] = None

    def connect(self):
        """Establish connection to the SQLite database."""
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row  # Enable column access by name
        return self.conn

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def create_schema(self):
        """
        Create database schema from schema.sql file.

        Raises:
            FileNotFoundError: If schema.sql is not found
        """
        if not self.schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {self.schema_path}")

        with open(self.schema_path, 'r') as f:
            schema_sql = f.read()

        if not self.conn:
            self.connect()

        cursor = self.conn.cursor()

        # Execute schema SQL (may contain multiple statements)
        cursor.executescript(schema_sql)
        self.conn.commit()

        print(f"✓ Database schema created successfully at {self.db_path}")

    def insert_tickers(self, tickers_df: pd.DataFrame):
        """
        Insert ticker data into the tickers table.

        Args:
            tickers_df: DataFrame containing ticker information
        """
        if not self.conn:
            self.connect()

        # Insert tickers
        tickers_df.to_sql('tickers', self.conn, if_exists='append', index=False)
        self.conn.commit()

        print(f"✓ Inserted {len(tickers_df)} tickers")

    def insert_market_data(self, market_df: pd.DataFrame, tickers_df: pd.DataFrame):
        """
        Insert market data into the prices table.

        Args:
            market_df: DataFrame containing market data
            tickers_df: DataFrame containing ticker information (for mapping)
        """
        if not self.conn:
            self.connect()

        # Create a mapping from ticker symbol to ticker_id
        ticker_map = dict(zip(tickers_df['symbol'], tickers_df['ticker_id']))

        # Prepare data for insertion
        prices_data = market_df.copy()
        prices_data['ticker_id'] = prices_data['ticker'].map(ticker_map)

        # Select and reorder columns for the prices table
        prices_data = prices_data[[
            'timestamp', 'ticker_id', 'open', 'high', 'low', 'close', 'volume'
        ]]

        # Convert timestamp to string format for SQLite
        prices_data['timestamp'] = prices_data['timestamp'].astype(str)

        # Insert into prices table
        prices_data.to_sql('prices', self.conn, if_exists='append', index=False)
        self.conn.commit()

        print(f"✓ Inserted {len(prices_data)} price records")

    def query_ticker_data_by_date_range(
        self, ticker_symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        Query 1: Retrieve all data for a given ticker and date range.

        Args:
            ticker_symbol: Ticker symbol (e.g., 'AAPL')
            start_date: Start date (e.g., '2025-11-17')
            end_date: End date (e.g., '2025-11-18')

        Returns:
            DataFrame with price data for the ticker in the date range
        """
        query = """
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
        WHERE t.symbol = ?
          AND p.timestamp >= ?
          AND p.timestamp <= ?
        ORDER BY p.timestamp
        """

        if not self.conn:
            self.connect()

        start_time = time.time()
        result = pd.read_sql_query(
            query,
            self.conn,
            params=(ticker_symbol, start_date, end_date)
        )
        elapsed = time.time() - start_time

        print(f"Query 1 executed in {elapsed:.4f} seconds")
        return result

    def query_average_daily_volume(self) -> pd.DataFrame:
        """
        Query 2: Calculate average daily volume per ticker.

        Returns:
            DataFrame with ticker symbol and average daily volume
        """
        query = """
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
        """

        if not self.conn:
            self.connect()

        start_time = time.time()
        result = pd.read_sql_query(query, self.conn)
        elapsed = time.time() - start_time

        print(f"Query 2 executed in {elapsed:.4f} seconds")
        return result

    def query_top_tickers_by_return(
        self, start_date: str = None, end_date: str = None, top_n: int = 3
    ) -> pd.DataFrame:
        """
        Query 3: Identify the top N tickers by return over a given period.

        Args:
            start_date: Start date for return calculation
            end_date: End date for return calculation
            top_n: Number of top tickers to return (default: 3)

        Returns:
            DataFrame with top N tickers by return
        """
        # If no dates specified, use the full dataset range
        date_filter = ""
        params = []

        if start_date and end_date:
            date_filter = "WHERE p.timestamp >= ? AND p.timestamp <= ?"
            params = [start_date, end_date]

        query = f"""
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
            {date_filter}
            GROUP BY ticker_id
        ) times
        JOIN tickers t ON times.ticker_id = t.ticker_id
        JOIN prices p1 ON times.ticker_id = p1.ticker_id AND times.first_time = p1.timestamp
        JOIN prices p2 ON times.ticker_id = p2.ticker_id AND times.last_time = p2.timestamp
        ORDER BY return_pct DESC
        LIMIT ?
        """

        if not self.conn:
            self.connect()

        params.append(top_n)
        start_time = time.time()
        result = pd.read_sql_query(query, self.conn, params=params)
        elapsed = time.time() - start_time

        print(f"Query 3 executed in {elapsed:.4f} seconds")
        return result

    def query_daily_first_last_prices(self) -> pd.DataFrame:
        """
        Query 4: Find the first and last trade price for each ticker per day.

        Returns:
            DataFrame with first and last prices for each ticker per day
        """
        query = """
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
        """

        if not self.conn:
            self.connect()

        start_time = time.time()
        result = pd.read_sql_query(query, self.conn)
        elapsed = time.time() - start_time

        # Rename columns for clarity
        result = result.rename(columns={
            'first_prices.first_price': 'first_price',
            'first_prices.first_time': 'first_time',
            'last_prices.last_price': 'last_price',
            'last_prices.last_time': 'last_time'
        })

        print(f"Query 4 executed in {elapsed:.4f} seconds")
        return result

    def get_database_size(self) -> int:
        """
        Get the size of the database file in bytes.

        Returns:
            Database file size in bytes
        """
        if self.db_path.exists():
            return self.db_path.stat().st_size
        return 0


def main():
    """Demonstrate SQLite storage and querying."""
    from data_loader import DataLoader

    # Load data
    print("Loading data...")
    loader = DataLoader()
    market_df, tickers_df = loader.load_and_validate()

    # Initialize SQLite storage
    storage = SQLiteStorage()

    # Remove existing database for fresh start
    if storage.db_path.exists():
        storage.db_path.unlink()
        print("Removed existing database")

    # Create schema and insert data
    print("\nCreating database schema...")
    storage.create_schema()

    print("\nInserting data...")
    storage.insert_tickers(tickers_df)
    storage.insert_market_data(market_df, tickers_df)

    # Run queries
    print("\n" + "="*60)
    print("QUERY 1: Retrieve TSLA data for 2025-11-17 to 2025-11-18")
    print("="*60)
    result1 = storage.query_ticker_data_by_date_range(
        'TSLA', '2025-11-17', '2025-11-18 23:59:59'
    )
    print(result1.head(10))
    print(f"Total rows: {len(result1)}")

    print("\n" + "="*60)
    print("QUERY 2: Average daily volume per ticker")
    print("="*60)
    result2 = storage.query_average_daily_volume()
    print(result2)

    print("\n" + "="*60)
    print("QUERY 3: Top 3 tickers by return")
    print("="*60)
    result3 = storage.query_top_tickers_by_return(top_n=3)
    print(result3)

    print("\n" + "="*60)
    print("QUERY 4: First and last trade prices per ticker per day")
    print("="*60)
    result4 = storage.query_daily_first_last_prices()
    print(result4.head(10))
    print(f"Total rows: {len(result4)}")

    # Report database size
    db_size = storage.get_database_size()
    print(f"\n✓ Database size: {db_size:,} bytes ({db_size / 1024:.2f} KB)")

    storage.close()


if __name__ == "__main__":
    main()
