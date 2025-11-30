"""
Unit tests for parquet_storage module.
"""

import pytest
import pandas as pd
from pathlib import Path
import sys
import tempfile
import shutil

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_loader import DataLoader
from parquet_storage import ParquetStorage


class TestParquetStorage:
    """Test suite for ParquetStorage class."""

    @pytest.fixture
    def temp_parquet_dir(self):
        """Create a temporary directory for Parquet files."""
        temp_dir = tempfile.mkdtemp()
        parquet_dir = Path(temp_dir) / "test_market_data"
        yield parquet_dir
        # Cleanup
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def storage(self, temp_parquet_dir):
        """Create a ParquetStorage instance with test directory."""
        return ParquetStorage(parquet_dir=temp_parquet_dir)

    @pytest.fixture
    def sample_data(self):
        """Load sample data for testing."""
        loader = DataLoader()
        market_df, tickers_df = loader.load_and_validate()
        return market_df, tickers_df

    def test_write_partitioned_data(self, storage, sample_data):
        """Test writing partitioned Parquet data."""
        market_df, tickers_df = sample_data

        storage.write_partitioned_data(market_df, tickers_df)

        # Verify directory was created
        assert storage.parquet_dir.exists()

        # Verify partitions were created
        partitions = list(storage.parquet_dir.glob('ticker=*'))
        assert len(partitions) > 0

        # Verify all expected tickers have partitions
        expected_tickers = set(tickers_df['symbol'].unique())
        partition_tickers = {p.name.split('=')[1] for p in partitions}
        assert expected_tickers == partition_tickers

    def test_partition_structure(self, storage, sample_data):
        """Test that partitions contain Parquet files."""
        market_df, tickers_df = sample_data

        storage.write_partitioned_data(market_df, tickers_df)

        # Check each partition has parquet files
        for partition in storage.parquet_dir.glob('ticker=*'):
            parquet_files = list(partition.glob('*.parquet'))
            assert len(parquet_files) > 0

    def test_read_all_data(self, storage, sample_data):
        """Test reading all data from Parquet."""
        market_df, tickers_df = sample_data

        storage.write_partitioned_data(market_df, tickers_df)
        result = storage.read_all_data()

        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(market_df)

        # Check essential columns are present
        assert 'timestamp' in result.columns
        assert 'ticker' in result.columns
        assert 'close' in result.columns

    def test_query_ticker_data_by_date_range(self, storage, sample_data):
        """Test querying specific ticker by date range."""
        market_df, tickers_df = sample_data

        storage.write_partitioned_data(market_df, tickers_df)

        result = storage.query_ticker_data_by_date_range(
            'AAPL', '2025-11-17', '2025-11-18 23:59:59'
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert all(result['ticker'] == 'AAPL')

        # Verify date range
        result['timestamp'] = pd.to_datetime(result['timestamp'])
        assert result['timestamp'].min() >= pd.to_datetime('2025-11-17')
        assert result['timestamp'].max() <= pd.to_datetime('2025-11-18 23:59:59')

    def test_compute_rolling_average(self, storage, sample_data):
        """Test computing rolling average."""
        market_df, tickers_df = sample_data

        storage.write_partitioned_data(market_df, tickers_df)

        result = storage.compute_rolling_average('AAPL', window=5, column='close')

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert 'close_rolling_5' in result.columns

        # First few values should be NaN due to window
        assert result['close_rolling_5'].iloc[:4].isna().all()

        # Later values should be valid
        assert not result['close_rolling_5'].iloc[5:].isna().all()

    def test_compute_rolling_volatility(self, storage, sample_data):
        """Test computing rolling volatility."""
        market_df, tickers_df = sample_data

        storage.write_partitioned_data(market_df, tickers_df)

        result = storage.compute_rolling_volatility(window=5)

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert 'rolling_volatility' in result.columns
        assert 'return' in result.columns
        assert 'ticker' in result.columns

        # Check all tickers are present
        expected_tickers = set(tickers_df['symbol'].unique())
        actual_tickers = set(result['ticker'].unique())
        assert expected_tickers == actual_tickers

    def test_get_storage_size(self, storage, sample_data):
        """Test getting Parquet storage size."""
        market_df, tickers_df = sample_data

        storage.write_partitioned_data(market_df, tickers_df)

        size = storage.get_storage_size()

        assert size > 0
        assert isinstance(size, int)

    def test_get_partition_info(self, storage, sample_data):
        """Test getting partition information."""
        market_df, tickers_df = sample_data

        storage.write_partitioned_data(market_df, tickers_df)

        info = storage.get_partition_info()

        assert isinstance(info, pd.DataFrame)
        assert len(info) == len(tickers_df)
        assert 'ticker' in info.columns
        assert 'file_count' in info.columns
        assert 'size_bytes' in info.columns

        # All partitions should have files
        assert all(info['file_count'] > 0)
        assert all(info['size_bytes'] > 0)

    def test_data_integrity_after_roundtrip(self, storage, sample_data):
        """Test data integrity after write and read."""
        market_df, tickers_df = sample_data

        storage.write_partitioned_data(market_df, tickers_df)
        result = storage.read_all_data()

        # Convert both to same types for comparison
        market_df_sorted = market_df.sort_values(['ticker', 'timestamp']).reset_index(drop=True)
        result_sorted = result.sort_values(['ticker', 'timestamp']).reset_index(drop=True)

        # Check row counts match
        assert len(result_sorted) == len(market_df_sorted)

        # Check key columns match
        assert all(result_sorted['ticker'] == market_df_sorted['ticker'])

    def test_query_nonexistent_ticker(self, storage, sample_data):
        """Test querying a ticker that doesn't exist."""
        market_df, tickers_df = sample_data

        storage.write_partitioned_data(market_df, tickers_df)

        result = storage.query_ticker_data_by_date_range(
            'INVALID', '2025-11-17', '2025-11-18'
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
