import pytest
from unittest.mock import patch, MagicMock
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ingestion'))

from tiingo_crypto import fetch_tiingo_crypto


class TestFetchTiingoCrypto:

    def test_raises_if_api_key_missing(self):
        """ValueError raised when TIINGO_API_KEY not set"""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="Missing TIINGO_API_KEY"):
                fetch_tiingo_crypto()

    def test_raises_if_bucket_name_missing(self):
        """ValueError raised when GCP_BUCKET_NAME not set"""
        with patch.dict(os.environ, {'TIINGO_API_KEY': 'fake_key'}, clear=True):
            with pytest.raises(ValueError, match="Missing TIINGO_API_KEY or GCP_BUCKET_NAME"):
                fetch_tiingo_crypto()

    @patch('tiingo_crypto.storage.Client')
    @patch('tiingo_crypto.requests.get')
    def test_calls_api_for_each_ticker(self, mock_get, mock_storage):
        """requests.get called once per crypto ticker"""
        mock_response = MagicMock()
        mock_response.json.return_value = [{"ticker": "btcusd", "priceData": []}]
        mock_get.return_value = mock_response

        mock_bucket = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket

        with patch.dict(os.environ, {
            'TIINGO_API_KEY': 'fake_key',
            'GCP_BUCKET_NAME': 'fake_bucket'
        }):
            fetch_tiingo_crypto()

        assert mock_get.call_count == 4

    @patch('tiingo_crypto.storage.Client')
    @patch('tiingo_crypto.requests.get')
    def test_uploads_to_correct_gcs_path(self, mock_get, mock_storage):
        """Each ticker uploaded to tiingo/crypto/ path in GCS"""
        mock_response = MagicMock()
        mock_response.json.return_value = [{"ticker": "btcusd"}]
        mock_get.return_value = mock_response

        mock_bucket = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket

        with patch.dict(os.environ, {
            'TIINGO_API_KEY': 'fake_key',
            'GCP_BUCKET_NAME': 'fake_bucket'
        }):
            fetch_tiingo_crypto()

        blob_paths = [call.args[0] for call in mock_bucket.blob.call_args_list]
        assert all(path.startswith("tiingo/crypto/") for path in blob_paths)
        assert all(path.endswith(".json") for path in blob_paths)

    @patch('tiingo_crypto.storage.Client')
    @patch('tiingo_crypto.requests.get')
    def test_continues_if_one_ticker_fails(self, mock_get, mock_storage):
        """Pipeline does not crash if one ticker API call fails"""
        mock_get.side_effect = Exception("API error")

        mock_bucket = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket

        with patch.dict(os.environ, {
            'TIINGO_API_KEY': 'fake_key',
            'GCP_BUCKET_NAME': 'fake_bucket'
        }):
            fetch_tiingo_crypto()  # Should not raise
