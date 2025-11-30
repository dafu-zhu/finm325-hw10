"""
Parquet storage module for multi-ticker market data.

This module provides functionality to convert market data to Parquet format,
partition by ticker, and execute analytical queries on columnar data.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Optional
import time
import shutil


class ParquetStorage:
    """Manages Parquet storage and querying for market data."""

    def __init__(self, parquet_dir: Path = None):
        """
        Initialize Parquet storage.

        Args:
            parquet_dir: Directory for partitioned Parquet files.
                        Defaults to 'market_data' in the hw10 directory.
        """
        if parquet_dir is None:
            parquet_dir = Path(__file__).parent / "market_data"

        self.parquet_dir = Path(parquet_dir)

    def write_partitioned_data(self, market_df: pd.DataFrame, tickers_df: pd.DataFrame):
        """
        Convert market data to Parquet format and partition by ticker.

        Args:
            market_df: DataFrame containing market data
            tickers_df: DataFrame containing ticker information
        """
        # Remove existing directory if it exists
        if self.parquet_dir.exists():
            shutil.rmtree(self.parquet_dir)
            print(f"Removed existing Parquet directory: {self.parquet_dir}")

        # Create directory
        self.parquet_dir.mkdir(parents=True, exist_ok=True)

        # Merge ticker information with market data
        merged_df = market_df.merge(
            tickers_df[['ticker_id', 'symbol', 'name', 'exchange']],
            left_on='ticker',
            right_on='symbol',
            how='left'
        )

        # Drop duplicate symbol column and rename ticker to symbol for clarity
        merged_df = merged_df.drop(columns=['ticker'])
        merged_df = merged_df.rename(columns={'symbol': 'ticker'})

        # Ensure timestamp is datetime
        merged_df['timestamp'] = pd.to_datetime(merged_df['timestamp'])

        # Convert to PyArrow Table for better control
        table = pa.Table.from_pandas(merged_df)

        # Write partitioned Parquet files
        pq.write_to_dataset(
            table,
            root_path=str(self.parquet_dir),
            partition_cols=['ticker'],
            compression='snappy',
            use_dictionary=True,
            write_statistics=True
        )

        print(f"✓ Data written to Parquet format in {self.parquet_dir}")
        print(f"  Partitioned by ticker: {sorted(merged_df['ticker'].unique())}")

    def read_all_data(self) -> pd.DataFrame:
        """
        Read all data from the partitioned Parquet dataset.

        Returns:
            DataFrame containing all market data
        """
        if not self.parquet_dir.exists():
            raise FileNotFoundError(f"Parquet directory not found: {self.parquet_dir}")

        df = pd.read_parquet(str(self.parquet_dir))

        return df

    def query_ticker_data_by_date_range(
        self, ticker_symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        Query: Retrieve all data for a given ticker and date range.

        Args:
            ticker_symbol: Ticker symbol (e.g., 'AAPL')
            start_date: Start date (e.g., '2025-11-17')
            end_date: End date (e.g., '2025-11-18')

        Returns:
            DataFrame with price data for the ticker in the date range
        """
        if not self.parquet_dir.exists():
            raise FileNotFoundError(f"Parquet directory not found: {self.parquet_dir}")

        start_time = time.time()

        # Read only the specific ticker partition
        ticker_path = self.parquet_dir / f"ticker={ticker_symbol}"

        if not ticker_path.exists():
            print(f"Warning: No data found for ticker {ticker_symbol}")
            return pd.DataFrame()

        # Read the partition
        df = pd.read_parquet(str(ticker_path))

        # Filter by date range
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        mask = (df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)
        result = df[mask].sort_values('timestamp').reset_index(drop=True)

        elapsed = time.time() - start_time
        print(f"Parquet query executed in {elapsed:.4f} seconds")

        return result

    def compute_rolling_average(
        self, ticker_symbol: str, window: int = 5, column: str = 'close'
    ) -> pd.DataFrame:
        """
        Compute rolling average for a specific ticker.

        Args:
            ticker_symbol: Ticker symbol (e.g., 'AAPL')
            window: Rolling window size (default: 5)
            column: Column to compute average on (default: 'close')

        Returns:
            DataFrame with rolling average
        """
        if not self.parquet_dir.exists():
            raise FileNotFoundError(f"Parquet directory not found: {self.parquet_dir}")

        start_time = time.time()

        # Read the specific ticker partition
        ticker_path = self.parquet_dir / f"ticker={ticker_symbol}"

        if not ticker_path.exists():
            print(f"Warning: No data found for ticker {ticker_symbol}")
            return pd.DataFrame()

        df = pd.read_parquet(str(ticker_path))

        # Sort by timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp').reset_index(drop=True)

        # Compute rolling average
        df[f'{column}_rolling_{window}'] = df[column].rolling(window=window).mean()

        elapsed = time.time() - start_time
        print(f"Rolling average computed in {elapsed:.4f} seconds")

        return df[['timestamp', column, f'{column}_rolling_{window}']]

    def compute_rolling_volatility(self, window: int = 5) -> pd.DataFrame:
        """
        Compute rolling N-day volatility (standard deviation of returns) for each ticker.

        Args:
            window: Rolling window size in periods (default: 5)

        Returns:
            DataFrame with rolling volatility for each ticker
        """
        if not self.parquet_dir.exists():
            raise FileNotFoundError(f"Parquet directory not found: {self.parquet_dir}")

        start_time = time.time()

        # Read all data
        df = self.read_all_data()

        # Ensure proper datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Sort by ticker and timestamp
        df = df.sort_values(['ticker', 'timestamp']).reset_index(drop=True)

        # Calculate returns
        df['return'] = df.groupby('ticker')['close'].pct_change()

        # Calculate rolling volatility (standard deviation of returns)
        df['rolling_volatility'] = df.groupby('ticker')['return'].transform(
            lambda x: x.rolling(window=window).std()
        )

        # Select relevant columns
        result = df[['timestamp', 'ticker', 'close', 'return', 'rolling_volatility']].copy()

        elapsed = time.time() - start_time
        print(f"Rolling volatility computed in {elapsed:.4f} seconds")

        return result

    def get_storage_size(self) -> int:
        """
        Get the total size of the Parquet directory in bytes.

        Returns:
            Total size in bytes
        """
        if not self.parquet_dir.exists():
            return 0

        total_size = 0
        for path in self.parquet_dir.rglob('*.parquet'):
            total_size += path.stat().st_size

        return total_size

    def get_partition_info(self) -> pd.DataFrame:
        """
        Get information about partitions.

        Returns:
            DataFrame with partition information (ticker, file count, size)
        """
        if not self.parquet_dir.exists():
            return pd.DataFrame()

        partitions = []

        for ticker_dir in sorted(self.parquet_dir.glob('ticker=*')):
            ticker_name = ticker_dir.name.split('=')[1]
            parquet_files = list(ticker_dir.glob('*.parquet'))
            file_count = len(parquet_files)
            total_size = sum(f.stat().st_size for f in parquet_files)

            partitions.append({
                'ticker': ticker_name,
                'file_count': file_count,
                'size_bytes': total_size,
                'size_kb': total_size / 1024
            })

        return pd.DataFrame(partitions)


def main():
    """Demonstrate Parquet storage and querying."""
    from data_loader import DataLoader

    # Load data
    print("Loading data...")
    loader = DataLoader()
    market_df, tickers_df = loader.load_and_validate()

    # Initialize Parquet storage
    storage = ParquetStorage()

    # Write partitioned data
    print("\nWriting data to Parquet format...")
    storage.write_partitioned_data(market_df, tickers_df)

    # Show partition info
    print("\nPartition Information:")
    partition_info = storage.get_partition_info()
    print(partition_info)

    # Run queries
    print("\n" + "="*60)
    print("QUERY 1: Retrieve AAPL data and compute 5-period rolling average")
    print("="*60)
    result1 = storage.compute_rolling_average('AAPL', window=5, column='close')
    print(result1.head(10))
    print(f"Total rows: {len(result1)}")

    print("\n" + "="*60)
    print("QUERY 2: Compute 5-period rolling volatility for all tickers")
    print("="*60)
    result2 = storage.compute_rolling_volatility(window=5)
    print(result2.groupby('ticker').head(10))
    print(f"Total rows: {len(result2)}")

    print("\n" + "="*60)
    print("QUERY 3: Compare query time - Retrieve TSLA data by date range")
    print("="*60)
    result3 = storage.query_ticker_data_by_date_range(
        'TSLA', '2025-11-17', '2025-11-18 23:59:59'
    )
    print(result3.head(10))
    print(f"Total rows: {len(result3)}")

    # Report storage size
    total_size = storage.get_storage_size()
    print(f"\n✓ Total Parquet storage size: {total_size:,} bytes ({total_size / 1024:.2f} KB)")


if __name__ == "__main__":
    main()
