import pytest
from unittest.mock import patch, MagicMock
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ingestion'))

from tiingo_stocks import fetch_tiingo_stocks


class TestFetchTiingoStocks:

    def test_raises_if_api_key_missing(self):
        """ValueError raised when TIINGO_API_KEY not set"""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="Missing TIINGO_API_KEY"):
                fetch_tiingo_stocks()

    def test_raises_if_bucket_name_missing(self):
        """ValueError raised when GCP_BUCKET_NAME not set"""
        with patch.dict(os.environ, {'TIINGO_API_KEY': 'fake_key'}, clear=True):
            with pytest.raises(ValueError, match="Missing TIINGO_API_KEY or GCP_BUCKET_NAME"):
                fetch_tiingo_stocks()

    @patch('tiingo_stocks.storage.Client')
    @patch('tiingo_stocks.requests.get')
    def test_calls_correct_url_for_each_stock(self, mock_get, mock_storage):
        """requests.get called with correct Tiingo URL for each stock"""
        mock_response = MagicMock()
        mock_response.json.return_value = [{"close": 254.23}]
        mock_get.return_value = mock_response

        mock_bucket = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket

        with patch.dict(os.environ, {
            'TIINGO_API_KEY': 'fake_key',
            'GCP_BUCKET_NAME': 'fake_bucket'
        }):
            fetch_tiingo_stocks()

        called_urls = [call.args[0] for call in mock_get.call_args_list]
        assert any("AAPL" in url for url in called_urls)
        assert any("MSFT" in url for url in called_urls)
        assert len(called_urls) == 5

    @patch('tiingo_stocks.storage.Client')
    @patch('tiingo_stocks.requests.get')
    def test_uploads_to_correct_gcs_path(self, mock_get, mock_storage):
        """Each stock uploaded to tiingo/stocks/ path in GCS"""
        mock_response = MagicMock()
        mock_response.json.return_value = [{"close": 254.23}]
        mock_get.return_value = mock_response

        mock_bucket = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket

        with patch.dict(os.environ, {
            'TIINGO_API_KEY': 'fake_key',
            'GCP_BUCKET_NAME': 'fake_bucket'
        }):
            fetch_tiingo_stocks()

        blob_paths = [call.args[0] for call in mock_bucket.blob.call_args_list]
        assert all(path.startswith("tiingo/stocks/") for path in blob_paths)
        assert all(path.endswith(".json") for path in blob_paths)

    @patch('tiingo_stocks.storage.Client')
    @patch('tiingo_stocks.requests.get')
    def test_continues_if_one_stock_fails(self, mock_get, mock_storage):
        """Pipeline does not crash if one stock API call fails"""
        mock_get.side_effect = Exception("API error")

        mock_bucket = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket

        with patch.dict(os.environ, {
            'TIINGO_API_KEY': 'fake_key',
            'GCP_BUCKET_NAME': 'fake_bucket'
        }):
            fetch_tiingo_stocks()  # Should not raise
