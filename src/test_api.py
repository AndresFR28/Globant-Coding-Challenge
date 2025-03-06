from api import api, db, DepartmentSchema, JobSchema, EmployeeSchema
import pytest
from unittest.mock import patch
import pandas as pd

api.config['TESTING'] = True

@pytest.fixture
def client():
    api.config['TESTING'] = True
    with api.test_client() as client:
        yield client

def test_home(client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "<h1>Globant API<h1>" in resp.data.decode()

@patch("os.path.exists")
def test_upload_historical_data_files_dont_exist(mock_exists, client):
    
    mock_exists.return_value = False
    
    resp = client.post('/api/v1/upload_historical_data')

    assert resp.status_code == 200
    assert "ERROR: Job CSV or Department CSV are not present on the path. No data was uploaded - 500" in resp.data.decode()

@patch("os.path.exists")
@patch("os.path.getsize")
def test_upload_historical_data_files_exist_empty(mock_getsize, mock_exists, client):
    
    mock_exists.return_value = True
    mock_getsize.return_value = 0
    
    resp = client.post('/api/v1/upload_historical_data')

    assert resp.status_code == 200
    assert "ERROR: Job CSV or Department CSV are empty. No data was uploaded - 500" in resp.data.decode()