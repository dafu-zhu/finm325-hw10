"""
Unit tests for sqlite_storage module.
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
from sqlite_storage import SQLiteStorage


class TestSQLiteStorage:
    """Test suite for SQLiteStorage class."""

    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database path."""
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "test_market_data.db"
        yield db_path
        # Cleanup
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def storage(self, temp_db_path):
        """Create a SQLiteStorage instance with test database."""
        return SQLiteStorage(db_path=temp_db_path)

    @pytest.fixture
    def sample_data(self):
        """Load sample data for testing."""
        loader = DataLoader()
        market_df, tickers_df = loader.load_and_validate()
        return market_df, tickers_df

    def test_create_schema(self, storage):
        """Test database schema creation."""
        storage.create_schema()

        # Verify database file was created
        assert storage.db_path.exists()

        # Verify tables were created
        storage.connect()
        cursor = storage.conn.cursor()

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]

        assert 'tickers' in tables
        assert 'prices' in tables

        storage.close()

    def test_insert_tickers(self, storage, sample_data):
        """Test inserting ticker data."""
        _, tickers_df = sample_data

        storage.create_schema()
        storage.insert_tickers(tickers_df)

        # Verify data was inserted
        result = pd.read_sql_query("SELECT * FROM tickers", storage.conn)

        assert len(result) == len(tickers_df)
        assert set(result['symbol']) == set(tickers_df['symbol'])

        storage.close()

    def test_insert_market_data(self, storage, sample_data):
        """Test inserting market data."""
        market_df, tickers_df = sample_data

        storage.create_schema()
        storage.insert_tickers(tickers_df)
        storage.insert_market_data(market_df, tickers_df)

        # Verify data was inserted
        result = pd.read_sql_query("SELECT COUNT(*) as count FROM prices", storage.conn)

        assert result['count'].iloc[0] == len(market_df)

        storage.close()

    def test_query_ticker_data_by_date_range(self, storage, sample_data):
        """Test querying ticker data by date range."""
        market_df, tickers_df = sample_data

        storage.create_schema()
        storage.insert_tickers(tickers_df)
        storage.insert_market_data(market_df, tickers_df)

        # Query AAPL data
        result = storage.query_ticker_data_by_date_range(
            'AAPL', '2025-11-17', '2025-11-18 23:59:59'
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert all(result['symbol'] == 'AAPL')

        # Verify date range
        result['timestamp'] = pd.to_datetime(result['timestamp'])
        assert result['timestamp'].min() >= pd.to_datetime('2025-11-17')
        assert result['timestamp'].max() <= pd.to_datetime('2025-11-18 23:59:59')

        storage.close()

    def test_query_average_daily_volume(self, storage, sample_data):
        """Test average daily volume query."""
        market_df, tickers_df = sample_data

        storage.create_schema()
        storage.insert_tickers(tickers_df)
        storage.insert_market_data(market_df, tickers_df)

        result = storage.query_average_daily_volume()

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert 'symbol' in result.columns
        assert 'avg_daily_volume' in result.columns

        # All tickers should be present
        assert len(result) == len(tickers_df)

        # Volumes should be positive
        assert all(result['avg_daily_volume'] > 0)

        storage.close()

    def test_query_top_tickers_by_return(self, storage, sample_data):
        """Test top tickers by return query."""
        market_df, tickers_df = sample_data

        storage.create_schema()
        storage.insert_tickers(tickers_df)
        storage.insert_market_data(market_df, tickers_df)

        result = storage.query_top_tickers_by_return(top_n=3)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert 'symbol' in result.columns
        assert 'return_pct' in result.columns

        # Returns should be sorted in descending order
        returns = result['return_pct'].values
        assert all(returns[i] >= returns[i+1] for i in range(len(returns)-1))

        storage.close()

    def test_query_daily_first_last_prices(self, storage, sample_data):
        """Test daily first and last prices query."""
        market_df, tickers_df = sample_data

        storage.create_schema()
        storage.insert_tickers(tickers_df)
        storage.insert_market_data(market_df, tickers_df)

        result = storage.query_daily_first_last_prices()

        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert 'symbol' in result.columns
        assert 'trade_date' in result.columns

        # Check that we have expected columns
        expected_columns = ['symbol', 'trade_date', 'first_time', 'last_time']
        for col in expected_columns:
            assert col in result.columns

        storage.close()

    def test_get_database_size(self, storage, sample_data):
        """Test getting database file size."""
        market_df, tickers_df = sample_data

        storage.create_schema()
        storage.insert_tickers(tickers_df)
        storage.insert_market_data(market_df, tickers_df)

        size = storage.get_database_size()

        assert size > 0
        assert isinstance(size, int)

        storage.close()

    def test_foreign_key_relationship(self, storage, sample_data):
        """Test that foreign key relationship is maintained."""
        _, tickers_df = sample_data

        storage.create_schema()
        storage.insert_tickers(tickers_df)

        # Verify foreign key constraint exists
        storage.connect()
        cursor = storage.conn.cursor()

        cursor.execute("PRAGMA foreign_keys")
        fk_status = cursor.fetchone()

        # Query schema to check foreign key definition
        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='prices'")
        schema = cursor.fetchone()[0]

        assert 'FOREIGN KEY' in schema
        assert 'ticker_id' in schema

        storage.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
