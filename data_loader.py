"""
Data loader module for multi-ticker market data ingestion and validation.

This module provides functionality to load, validate, and normalize multi-ticker
OHLCV (Open, High, Low, Close, Volume) data from CSV files.
"""

import pandas as pd
from pathlib import Path
from typing import Tuple, Set


class DataLoader:
    """Handles loading and validation of multi-ticker market data."""

    def __init__(self, data_dir: Path = None):
        """
        Initialize the DataLoader.

        Args:
            data_dir: Directory containing the data files.
                     Defaults to the 'files' subdirectory.
        """
        if data_dir is None:
            data_dir = Path(__file__).parent / "files"
        self.data_dir = Path(data_dir)

    def load_tickers(self) -> pd.DataFrame:
        """
        Load the tickers reference data.

        Returns:
            DataFrame containing ticker information (ticker_id, symbol, name, exchange)

        Raises:
            FileNotFoundError: If tickers.csv is not found
        """
        ticker_path = self.data_dir / "tickers.csv"
        if not ticker_path.exists():
            raise FileNotFoundError(f"Tickers file not found: {ticker_path}")

        tickers_df = pd.read_csv(ticker_path)
        return tickers_df

    def load_market_data(self) -> pd.DataFrame:
        """
        Load multi-ticker market data from CSV.

        Returns:
            DataFrame containing market data with normalized columns and datetime format

        Raises:
            FileNotFoundError: If market_data_multi.csv is not found
        """
        market_data_path = self.data_dir / "market_data_multi.csv"
        if not market_data_path.exists():
            raise FileNotFoundError(f"Market data file not found: {market_data_path}")

        df = pd.read_csv(market_data_path)

        # Normalize the data
        df = self._normalize_data(df)

        return df

    def _normalize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize column names and ensure consistent datetime formatting.

        Args:
            df: Raw market data DataFrame

        Returns:
            Normalized DataFrame with proper datetime types
        """
        # Normalize column names (lowercase, strip whitespace)
        df.columns = df.columns.str.strip().str.lower()

        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # Sort by timestamp and ticker for consistency
        df = df.sort_values(['timestamp', 'ticker']).reset_index(drop=True)

        return df

    def validate_data(self, df: pd.DataFrame, tickers_df: pd.DataFrame) -> Tuple[bool, list]:
        """
        Validate the market data for completeness and consistency.

        Args:
            df: Market data DataFrame to validate
            tickers_df: Reference tickers DataFrame

        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues = []

        # Check for missing timestamps
        if df['timestamp'].isnull().any():
            missing_count = df['timestamp'].isnull().sum()
            issues.append(f"Found {missing_count} missing timestamps")

        # Check for missing prices (open, high, low, close)
        price_columns = ['open', 'high', 'low', 'close']
        for col in price_columns:
            if col in df.columns and df[col].isnull().any():
                missing_count = df[col].isnull().sum()
                issues.append(f"Found {missing_count} missing values in column '{col}'")

        # Check for missing volume (note: volume can legitimately be 0, but not null)
        if 'volume' in df.columns and df['volume'].isnull().any():
            missing_count = df['volume'].isnull().sum()
            issues.append(f"Found {missing_count} missing volume values")

        # Validate all expected tickers are present
        expected_tickers: Set[str] = set(tickers_df['symbol'].unique())
        actual_tickers: Set[str] = set(df['ticker'].unique())

        missing_tickers = expected_tickers - actual_tickers
        if missing_tickers:
            issues.append(f"Missing tickers in data: {missing_tickers}")

        extra_tickers = actual_tickers - expected_tickers
        if extra_tickers:
            issues.append(f"Unexpected tickers in data: {extra_tickers}")

        # Validate price relationships (high >= low, etc.)
        invalid_prices = df[df['high'] < df['low']]
        if len(invalid_prices) > 0:
            issues.append(f"Found {len(invalid_prices)} rows where high < low")

        is_valid = len(issues) == 0
        return is_valid, issues

    def load_and_validate(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load and validate both market data and ticker data.

        Returns:
            Tuple of (market_data_df, tickers_df)

        Raises:
            ValueError: If validation fails
        """
        # Load data
        tickers_df = self.load_tickers()
        market_df = self.load_market_data()

        # Validate
        is_valid, issues = self.validate_data(market_df, tickers_df)

        if not is_valid:
            error_msg = "Data validation failed:\n" + "\n".join(f"  - {issue}" for issue in issues)
            raise ValueError(error_msg)

        return market_df, tickers_df


def main():
    """Demonstrate loading and validation of market data."""
    loader = DataLoader()

    try:
        market_df, tickers_df = loader.load_and_validate()

        print("✓ Data loaded and validated successfully!")
        print(f"\nTickers loaded: {len(tickers_df)}")
        print(tickers_df)

        print(f"\nMarket data shape: {market_df.shape}")
        print(f"Date range: {market_df['timestamp'].min()} to {market_df['timestamp'].max()}")
        print(f"Tickers in data: {sorted(market_df['ticker'].unique())}")

        print("\nFirst few rows:")
        print(market_df.head(10))

        print("\nData summary:")
        print(market_df.describe())

    except Exception as e:
        print(f"✗ Error: {e}")
        raise


if __name__ == "__main__":
    main()
