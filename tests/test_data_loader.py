"""
Unit tests for data_loader module.
"""

import pytest
import pandas as pd
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_loader import DataLoader


class TestDataLoader:
    """Test suite for DataLoader class."""

    @pytest.fixture
    def loader(self):
        """Create a DataLoader instance."""
        return DataLoader()

    def test_load_tickers(self, loader):
        """Test loading tickers data."""
        tickers_df = loader.load_tickers()

        assert isinstance(tickers_df, pd.DataFrame)
        assert len(tickers_df) > 0
        assert 'symbol' in tickers_df.columns
        assert 'ticker_id' in tickers_df.columns
        assert 'name' in tickers_df.columns
        assert 'exchange' in tickers_df.columns

    def test_tickers_content(self, loader):
        """Test tickers data contains expected symbols."""
        tickers_df = loader.load_tickers()

        expected_symbols = {'AAPL', 'MSFT', 'GOOG', 'TSLA', 'AMZN'}
        actual_symbols = set(tickers_df['symbol'].unique())

        assert expected_symbols == actual_symbols

    def test_load_market_data(self, loader):
        """Test loading market data."""
        market_df = loader.load_market_data()

        assert isinstance(market_df, pd.DataFrame)
        assert len(market_df) > 0

        # Check required columns
        required_columns = {'timestamp', 'ticker', 'open', 'high', 'low', 'close', 'volume'}
        assert required_columns.issubset(set(market_df.columns))

    def test_market_data_normalization(self, loader):
        """Test that market data is normalized properly."""
        market_df = loader.load_market_data()

        # Timestamp should be datetime
        assert pd.api.types.is_datetime64_any_dtype(market_df['timestamp'])

        # Column names should be lowercase
        assert all(col == col.lower() for col in market_df.columns)

        # Data should be sorted
        assert market_df['timestamp'].is_monotonic_increasing or \
               (market_df.groupby('ticker')['timestamp'].is_monotonic_increasing.all())

    def test_validate_data_success(self, loader):
        """Test validation with valid data."""
        tickers_df = loader.load_tickers()
        market_df = loader.load_market_data()

        is_valid, issues = loader.validate_data(market_df, tickers_df)

        assert is_valid is True
        assert len(issues) == 0

    def test_validate_data_missing_timestamps(self, loader):
        """Test validation catches missing timestamps."""
        tickers_df = loader.load_tickers()
        market_df = loader.load_market_data()

        # Introduce missing timestamp
        market_df.loc[0, 'timestamp'] = pd.NaT

        is_valid, issues = loader.validate_data(market_df, tickers_df)

        assert is_valid is False
        assert any('missing timestamps' in issue.lower() for issue in issues)

    def test_validate_data_missing_prices(self, loader):
        """Test validation catches missing prices."""
        tickers_df = loader.load_tickers()
        market_df = loader.load_market_data()

        # Introduce missing price
        market_df.loc[0, 'close'] = None

        is_valid, issues = loader.validate_data(market_df, tickers_df)

        assert is_valid is False
        assert any('close' in issue.lower() for issue in issues)

    def test_validate_data_missing_ticker(self, loader):
        """Test validation catches missing tickers."""
        # Create tickers with an extra symbol
        tickers_df = loader.load_tickers()
        extra_ticker = pd.DataFrame([{
            'ticker_id': 999,
            'symbol': 'MISSING',
            'name': 'Missing Corp',
            'exchange': 'NASDAQ'
        }])
        tickers_df = pd.concat([tickers_df, extra_ticker], ignore_index=True)

        market_df = loader.load_market_data()

        is_valid, issues = loader.validate_data(market_df, tickers_df)

        assert is_valid is False
        assert any('missing tickers' in issue.lower() for issue in issues)

    def test_load_and_validate(self, loader):
        """Test combined load and validate operation."""
        market_df, tickers_df = loader.load_and_validate()

        assert isinstance(market_df, pd.DataFrame)
        assert isinstance(tickers_df, pd.DataFrame)
        assert len(market_df) > 0
        assert len(tickers_df) > 0

    def test_data_integrity(self, loader):
        """Test that high >= low for all records."""
        market_df = loader.load_market_data()

        invalid_rows = market_df[market_df['high'] < market_df['low']]
        assert len(invalid_rows) == 0, "Found rows where high < low"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
