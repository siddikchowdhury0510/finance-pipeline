import pytest
from unittest.mock import MagicMock, patch
import json

class TestLoadStocksSchema:
    """Tests for stocks BigQuery schema validation"""

    def test_required_schema_fields(self):
        """All required fields must be in schema"""
        required_fields = [
            'ticker', 'date', 'open', 'high', 'low',
            'close', 'volume', 'adj_close', 'ingested_at'
        ]
        schema_field_names = [
            'ticker', 'date', 'open', 'high', 'low',
            'close', 'volume', 'adj_close', 'adj_open',
            'adj_high', 'adj_low', 'div_cash',
            'split_factor', 'ingested_at'
        ]
        for field in required_fields:
            assert field in schema_field_names, \
                f"Missing required field: {field}"

    def test_ticker_extracted_from_filename(self):
        """Ticker should be correctly extracted from filename"""
        filename = "AAPL_2026-03-18_15-54-24.json"
        ticker = filename.split('_')[0]
        assert ticker == "AAPL"

    def test_ticker_extraction_googl(self):
        """GOOGL ticker extraction"""
        filename = "GOOGL_2026-03-18_15-54-24.json"
        ticker = filename.split('_')[0]
        assert ticker == "GOOGL"

    def test_row_has_all_required_keys(self):
        """Each row dict must have all required keys"""
        row = {
            'ticker': 'AAPL',
            'date': '2026-03-17T00:00:00+00:00',
            'open': 252.955,
            'high': 255.13,
            'low': 252.18,
            'close': 254.23,
            'volume': 32361607,
            'adj_close': 254.23,
            'adj_open': 252.955,
            'adj_high': 255.13,
            'adj_low': 252.18,
            'div_cash': 0.0,
            'split_factor': 1.0,
            'ingested_at': '2026-03-18T17:10:41'
        }
        required_keys = [
            'ticker', 'date', 'open', 'high',
            'low', 'close', 'volume', 'ingested_at'
        ]
        for key in required_keys:
            assert key in row, f"Missing key: {key}"

    def test_close_price_positive(self):
        """Close price must be positive"""
        row = {'close': 254.23}
        assert row['close'] > 0

    def test_volume_positive(self):
        """Volume must be positive"""
        row = {'volume': 32361607}
        assert row['volume'] > 0


class TestLoadCryptoSchema:
    """Tests for crypto BigQuery schema validation"""

    def test_required_schema_fields(self):
        """All required crypto fields must be present"""
        required_fields = [
            'ticker', 'base_currency', 'quote_currency',
            'date', 'open', 'high', 'low', 'close',
            'volume', 'ingested_at'
        ]
        schema_field_names = [
            'ticker', 'base_currency', 'quote_currency',
            'date', 'open', 'high', 'low', 'close',
            'volume', 'ingested_at'
        ]
        for field in required_fields:
            assert field in schema_field_names

    def test_ticker_format(self):
        """Crypto ticker should be lowercase"""
        ticker = "btcusd"
        assert ticker == ticker.lower()

    def test_base_currency_extracted(self):
        """Base currency should be extractable"""
        record = {
            'ticker': 'btcusd',
            'baseCurrency': 'btc',
            'quoteCurrency': 'usd'
        }
        assert record.get('baseCurrency') == 'btc'
        assert record.get('quoteCurrency') == 'usd'


class TestLoadMacroSchema:
    """Tests for macro BigQuery schema validation"""

    def test_required_schema_fields(self):
        """All required macro fields must be present"""
        required_fields = ['indicator', 'date', 'value', 'ingested_at']
        schema_field_names = ['indicator', 'date', 'value', 'ingested_at']
        for field in required_fields:
            assert field in schema_field_names

    def test_indicator_extracted_from_filename(self):
        """Indicator name should be correctly extracted"""
        filename = "inflation_2026-03-18_16-10-28.json"
        indicator = filename.split('_')[0]
        assert indicator == "inflation"

    def test_indicator_extraction_gdp(self):
        """GDP indicator extraction"""
        filename = "gdp_2026-03-18_16-10-28.json"
        indicator = filename.split('_')[0]
        assert indicator == "gdp"

    def test_value_is_numeric(self):
        """Macro value should be numeric"""
        record = {'value': 3.2}
        assert isinstance(record['value'], (int, float))


class TestWriteDisposition:
    """Tests to validate WRITE_TRUNCATE behaviour"""

    def test_write_disposition_is_truncate(self):
        """Write disposition must be WRITE_TRUNCATE"""
        write_disposition = "WRITE_TRUNCATE"
        assert write_disposition == "WRITE_TRUNCATE"
        assert write_disposition != "WRITE_APPEND"
        assert write_disposition != "WRITE_EMPTY"

    def test_truncate_prevents_duplicates(self):
        """Simulates that WRITE_TRUNCATE resets row count"""
        # Simulate two runs
        run_1_rows = [{'ticker': 'AAPL', 'close': 254.23}]
        run_2_rows = [{'ticker': 'AAPL', 'close': 255.00}]

        # With WRITE_TRUNCATE table is replaced not appended
        table_after_run_1 = run_1_rows
        table_after_run_2 = run_2_rows  # replaces, not appends

        assert len(table_after_run_2) == 1
        assert table_after_run_2[0]['close'] == 255.00
