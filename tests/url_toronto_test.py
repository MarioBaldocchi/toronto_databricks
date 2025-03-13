import pytest
import requests
from unittest.mock import patch, MagicMock
from toronto_bike.UrlToronto import UrlToronto

@pytest.fixture
def url_toronto():
    return UrlToronto(ubicacion_temporal='temp_dir')

@patch('toronto_bike.UrlToronto.requests.get')
def test_select_valid_urls(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "success": True,
        "result": {
            "resources": [
                {
                    "datastore_active": False,
                    "id": "resource_id_1",
                    "url": "https://example.com/bikeshare-ridership-2023.zip"
                }
            ]
        }
    }
    mock_get.return_value = mock_response

    valid_urls = UrlToronto.select_valid_urls()
    assert "https://example.com/bikeshare-ridership-2023.zip" in valid_urls

