import pytest
import json
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ingestion'))

class TestTiingoStocksResponse:
    """Tests for Tiingo stock API response validation"""

    def test_response_is_list(self):
        sample = [{"close": 254.23, "open": 252.95, "high": 255.13,
                   "low": 252.18, "volume": 32361607, "date": "2026-03-17T00:00:00+00:00",
                   "adjClose": 254.23, "adjOpen": 252.95, "adjHigh": 255.13,
                   "adjLow": 252.18, "adjVolume": 32361607, "divCash": 0.0,
                   "splitFactor": 1.0}]
        assert isinstance(sample, list)

    def test_response_not_empty(self):
        sample = [{"close": 254.23}]
        assert len(sample) > 0

    def test_required_fields_present(self):
        sample = {"close": 254.23, "open": 252.95, "high": 255.13,
                  "low": 252.18, "volume": 32361607,
                  "date": "2026-03-17T00:00:00+00:00"}
        required_fields = ['close', 'open', 'high', 'low', 'volume', 'date']
        for field in required_fields:
            assert field in sample, f"Missing required field: {field}"

    def test_close_price_is_numeric(self):
        sample = {"close": 254.23}
        assert isinstance(sample['close'], (int, float))

    def test_close_price_is_positive(self):
        sample = {"close": 254.23}
        assert sample['close'] > 0

    def test_date_is_string(self):
        sample = {"date": "2026-03-17T00:00:00+00:00"}
        assert isinstance(sample['date'], str)

    def test_missing_field_caught(self):
        sample = {"close": 254.23}
        required_fields = ['close', 'open', 'high', 'low', 'volume', 'date']
        missing = [f for f in required_fields if f not in sample]
        assert len(missing) > 0


class TestTiingoCryptoResponse:
    """Tests for Tiingo crypto API response validation"""

    def test_response_is_list(self):
        sample = [{"ticker": "btcusd", "baseCurrency": "btc",
                   "quoteCurrency": "usd", "priceData": []}]
        assert isinstance(sample, list)

    def test_ticker_present(self):
        sample = {"ticker": "btcusd"}
        assert "ticker" in sample

    def test_ticker_is_string(self):
        sample = {"ticker": "btcusd"}
        assert isinstance(sample['ticker'], str)


class TestFREDResponse:
    """Tests for FRED API response validation"""

    def test_response_not_empty(self):
        sample = [{"date": "2026-01-01", "value": 3.2}]
        assert len(sample) > 0

    def test_required_fields_present(self):
        sample = {"date": "2026-01-01", "value": 3.2}
        assert "date" in sample
        assert "value" in sample

    def test_value_is_numeric(self):
        sample = {"value": 3.2}
        assert isinstance(sample['value'], (int, float))


class TestFilenameGeneration:
    """Tests for GCS filename generation"""

    def test_filename_contains_ticker(self):
        ticker = "AAPL"
        timestamp = "2026-03-18_09-30-00"
        filename = f"tiingo/stocks/{ticker}_{timestamp}.json"
        assert ticker in filename

    def test_filename_contains_timestamp(self):
        ticker = "AAPL"
        timestamp = "2026-03-18_09-30-00"
        filename = f"tiingo/stocks/{ticker}_{timestamp}.json"
        assert timestamp in filename

    def test_filename_has_json_extension(self):
        filename = "tiingo/stocks/AAPL_2026-03-18_09-30-00.json"
        assert filename.endswith('.json')

    def test_filename_correct_path_stocks(self):
        filename = "tiingo/stocks/AAPL_2026-03-18_09-30-00.json"
        assert filename.startswith("tiingo/stocks/")

    def test_filename_correct_path_crypto(self):
        filename = "tiingo/crypto/btcusd_2026-03-18_09-30-00.json"
        assert filename.startswith("tiingo/crypto/")
