import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ingestion'))

from fred_macro import fetch_fred_macro


class TestFetchFredMacro:

    def test_raises_if_api_key_missing(self):
        """ValueError raised when FRED_API_KEY not set"""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="Missing FRED_API_KEY"):
                fetch_fred_macro()

    def test_raises_if_bucket_name_missing(self):
        """ValueError raised when GCP_BUCKET_NAME not set"""
        with patch.dict(os.environ, {'FRED_API_KEY': 'fake_key'}, clear=True):
            with pytest.raises(ValueError, match="Missing FRED_API_KEY or GCP_BUCKET_NAME"):
                fetch_fred_macro()

    @patch('fred_macro.storage.Client')
    @patch('fred_macro.Fred')
    def test_fetches_all_four_indicators(self, mock_fred_class, mock_storage):
        """get_series called for all four macro indicators"""
        mock_fred = MagicMock()
        mock_fred.get_series.return_value = pd.Series(
            [3.2, 3.1], index=pd.to_datetime(['2024-01-01', '2024-02-01'])
        )
        mock_fred_class.return_value = mock_fred

        mock_bucket = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket

        with patch.dict(os.environ, {
            'FRED_API_KEY': 'fake_key',
            'GCP_BUCKET_NAME': 'fake_bucket'
        }):
            fetch_fred_macro()

        assert mock_fred.get_series.call_count == 4

    @patch('fred_macro.storage.Client')
    @patch('fred_macro.Fred')
    def test_uploads_to_correct_gcs_path(self, mock_fred_class, mock_storage):
        """Each indicator uploaded to fred/ path in GCS"""
        mock_fred = MagicMock()
        mock_fred.get_series.return_value = pd.Series(
            [3.2], index=pd.to_datetime(['2024-01-01'])
        )
        mock_fred_class.return_value = mock_fred

        mock_bucket = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket

        with patch.dict(os.environ, {
            'FRED_API_KEY': 'fake_key',
            'GCP_BUCKET_NAME': 'fake_bucket'
        }):
            fetch_fred_macro()

        blob_paths = [call.args[0] for call in mock_bucket.blob.call_args_list]
        assert all(path.startswith("fred/") for path in blob_paths)
        assert all(path.endswith(".json") for path in blob_paths)

    @patch('fred_macro.storage.Client')
    @patch('fred_macro.Fred')
    def test_continues_if_one_indicator_fails(self, mock_fred_class, mock_storage):
        """Pipeline does not crash if one indicator fetch fails"""
        mock_fred = MagicMock()
        mock_fred.get_series.side_effect = Exception("FRED error")
        mock_fred_class.return_value = mock_fred

        mock_bucket = MagicMock()
        mock_storage.return_value.bucket.return_value = mock_bucket

        with patch.dict(os.environ, {
            'FRED_API_KEY': 'fake_key',
            'GCP_BUCKET_NAME': 'fake_bucket'
        }):
            fetch_fred_macro()  # Should not raise
