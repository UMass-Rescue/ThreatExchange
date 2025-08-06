# Copyright (c) Meta Platforms, Inc. and affiliates.

import typing as t

import pytest
from flask.testing import FlaskClient
from unittest.mock import Mock, patch

from threatexchange.signal_type.pdq.signal import PdqSignal
from threatexchange.signal_type.md5 import VideoMD5Signal
from threatexchange.exchanges.impl.static_sample import StaticSampleSignalExchangeAPI

from OpenMediaMatch.tests.utils import app, client, create_bank, add_hash_to_bank
from OpenMediaMatch.background_tasks import fetcher, build_index
from OpenMediaMatch.blueprints.matching import TMatchByBank
from OpenMediaMatch.persistence import get_storage
from OpenMediaMatch.storage import interface as iface


@pytest.fixture()
def client_with_sample_data(app) -> FlaskClient:
    storage = get_storage()
    storage.exchange_api_config_update(
        iface.SignalExchangeAPIConfig(StaticSampleSignalExchangeAPI)
    )
    storage.exchange_update(
        StaticSampleSignalExchangeAPI.get_config_cls()(
            name="SAMPLE",
            api=StaticSampleSignalExchangeAPI.get_name(),
            enabled=True,
        ),
        create=True,
    )
    fetcher.fetch_all(storage, storage.get_signal_type_configs())
    build_index.build_all_indices(storage, storage, storage)

    client = app.test_client()
    assert client.get("/status").status_code == 200
    return client


def test_threshold_lookup_missing_parameters(client: FlaskClient):
    """Test threshold_lookup with missing parameters returns 400 error."""
    response = client.get("/m/threshold_lookup")
    assert response.status_code == 400


def test_threshold_lookup_missing_signal(client: FlaskClient):
    """Test threshold_lookup with missing signal parameter returns 400 error."""
    response = client.get("/m/threshold_lookup?signal_type=clip&threshold=0.7")
    assert response.status_code == 400


def test_threshold_lookup_missing_signal_type(client: FlaskClient):
    """Test threshold_lookup with missing signal_type parameter returns 400 error."""
    response = client.get("/m/threshold_lookup?signal=test_hash&threshold=0.7")
    assert response.status_code == 400


def test_threshold_lookup_missing_threshold(client: FlaskClient):
    """Test threshold_lookup with missing threshold parameter returns 400 error."""
    response = client.get("/m/threshold_lookup?signal=test_hash&signal_type=clip")
    assert response.status_code == 400


def test_threshold_lookup_invalid_threshold(client: FlaskClient):
    """Test threshold_lookup with invalid threshold parameter returns 400 error."""
    response = client.get("/m/threshold_lookup?signal=test_hash&signal_type=clip&threshold=invalid")
    assert response.status_code == 400


def test_threshold_lookup_with_pdq_signal(client: FlaskClient):
    """Test threshold_lookup with PDQ signal type returns 400 error (not supported)."""
    response = client.get("/m/threshold_lookup?signal=test_hash&signal_type=pdq&threshold=0.7")
    assert response.status_code == 400
    assert "threshold_lookup not available for signal type 'pdq'" in response.json['error']


def test_threshold_lookup_with_vpdq_signal(client: FlaskClient):
    """Test threshold_lookup with VPDQ signal type returns 400 error (not supported)."""
    response = client.get("/m/threshold_lookup?signal=test_hash&signal_type=vpdq&threshold=0.7")
    assert response.status_code == 400
    assert "threshold_lookup not available for signal type 'vpdq'" in response.json['error']


def test_threshold_lookup_with_clip_signal_mocked(client: FlaskClient):
    """Test threshold_lookup with CLIP signal type using mocked index."""
    # Mock the index with query_threshold_index method
    mock_index = Mock()
    mock_index.query_threshold_index.return_value = [
        Mock(metadata=1, similarity_info=Mock(pretty_str=lambda: "0.5")),
        Mock(metadata=2, similarity_info=Mock(pretty_str=lambda: "0.8")),
    ]
    
    with patch('OpenMediaMatch.blueprints.matching._get_index', return_value=mock_index):
        with patch('OpenMediaMatch.blueprints.matching._validate_and_transform_signal_type'):
            with patch('OpenMediaMatch.blueprints.matching.get_storage') as mock_storage:
                # Mock bank content
                mock_content1 = Mock(id=1, enabled=True, bank=Mock(name="BANK_A"))
                mock_content2 = Mock(id=2, enabled=True, bank=Mock(name="BANK_B"))
                mock_storage.return_value.bank_content_get.return_value = [mock_content1, mock_content2]
                
                response = client.get("/m/threshold_lookup?signal=test_hash&signal_type=clip&threshold=0.7")
                
                assert response.status_code == 200
                results = response.json
                assert "BANK_A" in results
                assert "BANK_B" in results
                assert len(results["BANK_A"]) == 1
                assert len(results["BANK_B"]) == 1
                mock_index.query_threshold_index.assert_called_once_with("test_hash", 0.7)


def test_topk_lookup_missing_parameters(client: FlaskClient):
    """Test topk_lookup with missing parameters returns 400 error."""
    response = client.get("/m/topk_lookup")
    assert response.status_code == 400


def test_topk_lookup_missing_signal(client: FlaskClient):
    """Test topk_lookup with missing signal parameter returns 400 error."""
    response = client.get("/m/topk_lookup?signal_type=clip&k=5")
    assert response.status_code == 400


def test_topk_lookup_missing_signal_type(client: FlaskClient):
    """Test topk_lookup with missing signal_type parameter returns 400 error."""
    response = client.get("/m/topk_lookup?signal=test_hash&k=5")
    assert response.status_code == 400


def test_topk_lookup_missing_k(client: FlaskClient):
    """Test topk_lookup with missing k parameter returns 400 error."""
    response = client.get("/m/topk_lookup?signal=test_hash&signal_type=clip")
    assert response.status_code == 400


def test_topk_lookup_invalid_k(client: FlaskClient):
    """Test topk_lookup with invalid k parameter returns 400 error."""
    response = client.get("/m/topk_lookup?signal=test_hash&signal_type=clip&k=invalid")
    assert response.status_code == 400


def test_topk_lookup_invalid_max_threshold(client: FlaskClient):
    """Test topk_lookup with invalid max_threshold parameter returns 400 error."""
    response = client.get("/m/topk_lookup?signal=test_hash&signal_type=clip&k=5&max_threshold=invalid")
    assert response.status_code == 400


def test_topk_lookup_with_pdq_signal(client: FlaskClient):
    """Test topk_lookup with PDQ signal type returns 400 error (not supported)."""
    response = client.get("/m/topk_lookup?signal=test_hash&signal_type=pdq&k=5")
    assert response.status_code == 400
    assert "topk_lookup not available for signal type 'pdq'" in response.json['error']


def test_topk_lookup_with_vpdq_signal(client: FlaskClient):
    """Test topk_lookup with VPDQ signal type returns 400 error (not supported)."""
    response = client.get("/m/topk_lookup?signal=test_hash&signal_type=vpdq&k=5")
    assert response.status_code == 400
    assert "topk_lookup not available for signal type 'vpdq'" in response.json['error']


def test_topk_lookup_with_clip_signal_mocked(client: FlaskClient):
    """Test topk_lookup with CLIP signal type using mocked index."""
    # Mock the index with query_topk_index method
    mock_index = Mock()
    mock_index.query_topk_index.return_value = [
        Mock(metadata=1, similarity_info=Mock(pretty_str=lambda: "0.5")),
        Mock(metadata=2, similarity_info=Mock(pretty_str=lambda: "0.8")),
    ]
    
    with patch('OpenMediaMatch.blueprints.matching._get_index', return_value=mock_index):
        with patch('OpenMediaMatch.blueprints.matching._validate_and_transform_signal_type'):
            with patch('OpenMediaMatch.blueprints.matching.get_storage') as mock_storage:
                # Mock bank content
                mock_content1 = Mock(id=1, enabled=True, bank=Mock(name="BANK_A"))
                mock_content2 = Mock(id=2, enabled=True, bank=Mock(name="BANK_B"))
                mock_storage.return_value.bank_content_get.return_value = [mock_content1, mock_content2]
                
                response = client.get("/m/topk_lookup?signal=test_hash&signal_type=clip&k=5")
                
                assert response.status_code == 200
                results = response.json
                assert "BANK_A" in results
                assert "BANK_B" in results
                assert len(results["BANK_A"]) == 1
                assert len(results["BANK_B"]) == 1
                mock_index.query_topk_index.assert_called_once_with("test_hash", 5, None)


def test_topk_lookup_with_clip_signal_and_max_threshold_mocked(client: FlaskClient):
    """Test topk_lookup with CLIP signal type and max_threshold parameter using mocked index."""
    # Mock the index with query_topk_index method
    mock_index = Mock()
    mock_index.query_topk_index.return_value = [
        Mock(metadata=1, similarity_info=Mock(pretty_str=lambda: "0.5")),
        Mock(metadata=2, similarity_info=Mock(pretty_str=lambda: "0.8")),
    ]
    
    with patch('OpenMediaMatch.blueprints.matching._get_index', return_value=mock_index):
        with patch('OpenMediaMatch.blueprints.matching._validate_and_transform_signal_type'):
            with patch('OpenMediaMatch.blueprints.matching.get_storage') as mock_storage:
                # Mock bank content
                mock_content1 = Mock(id=1, enabled=True, bank=Mock(name="BANK_A"))
                mock_content2 = Mock(id=2, enabled=True, bank=Mock(name="BANK_B"))
                mock_storage.return_value.bank_content_get.return_value = [mock_content1, mock_content2]
                
                response = client.get("/m/topk_lookup?signal=test_hash&signal_type=clip&k=5&max_threshold=0.7")
                
                assert response.status_code == 200
                results = response.json
                assert "BANK_A" in results
                assert "BANK_B" in results
                assert len(results["BANK_A"]) == 1
                assert len(results["BANK_B"]) == 1
                mock_index.query_topk_index.assert_called_once_with("test_hash", 5, 0.7)


def test_threshold_lookup_index_not_ready(client: FlaskClient):
    """Test that 503 error is returned when index is not ready."""
    with patch('OpenMediaMatch.blueprints.matching._get_index', return_value=None):
        response = client.get("/m/threshold_lookup?signal=test_hash&signal_type=clip&threshold=0.7")
        assert response.status_code == 503
        assert "index not yet ready" in response.json['error']


def test_topk_lookup_index_not_ready(client: FlaskClient):
    """Test that 503 error is returned when index is not ready."""
    with patch('OpenMediaMatch.blueprints.matching._get_index', return_value=None):
        response = client.get("/m/topk_lookup?signal=test_hash&signal_type=clip&k=5")
        assert response.status_code == 503
        assert "index not yet ready" in response.json['error']


def test_threshold_lookup_invalid_signal(client: FlaskClient):
    """Test that 400 error is returned for invalid signal."""
    with patch('OpenMediaMatch.blueprints.matching._validate_and_transform_signal_type') as mock_validate:
        mock_signal_type = Mock()
        mock_signal_type.validate_signal_str.side_effect = ValueError("Invalid signal format")
        mock_validate.return_value = mock_signal_type
        
        response = client.get("/m/threshold_lookup?signal=invalid_hash&signal_type=clip&threshold=0.7")
        assert response.status_code == 400
        assert "invalid signal" in response.json['error']


def test_topk_lookup_invalid_signal(client: FlaskClient):
    """Test that 400 error is returned for invalid signal."""
    with patch('OpenMediaMatch.blueprints.matching._validate_and_transform_signal_type') as mock_validate:
        mock_signal_type = Mock()
        mock_signal_type.validate_signal_str.side_effect = ValueError("Invalid signal format")
        mock_validate.return_value = mock_signal_type
        
        response = client.get("/m/topk_lookup?signal=invalid_hash&signal_type=clip&k=5")
        assert response.status_code == 400
        assert "invalid signal" in response.json['error']


def test_threshold_lookup_response_format(client: FlaskClient):
    """Test that threshold_lookup returns the correct response format."""
    # Mock the index with query_threshold_index method
    mock_index = Mock()
    mock_index.query_threshold_index.return_value = [
        Mock(metadata=1, similarity_info=Mock(pretty_str=lambda: "0.5")),
        Mock(metadata=2, similarity_info=Mock(pretty_str=lambda: "0.8")),
    ]
    
    with patch('OpenMediaMatch.blueprints.matching._get_index', return_value=mock_index):
        with patch('OpenMediaMatch.blueprints.matching._validate_and_transform_signal_type'):
            with patch('OpenMediaMatch.blueprints.matching.get_storage') as mock_storage:
                # Mock bank content
                mock_content1 = Mock(id=1, enabled=True, bank=Mock(name="BANK_A"))
                mock_content2 = Mock(id=2, enabled=True, bank=Mock(name="BANK_B"))
                mock_storage.return_value.bank_content_get.return_value = [mock_content1, mock_content2]
                
                response = client.get("/m/threshold_lookup?signal=test_hash&signal_type=clip&threshold=0.7")
                
                assert response.status_code == 200
                results = response.json
                
                # Check response format matches TMatchByBank
                assert isinstance(results, dict)
                for bank_name, matches in results.items():
                    assert isinstance(bank_name, str)
                    assert isinstance(matches, list)
                    for match in matches:
                        assert "bank_content_id" in match
                        assert "distance" in match
                        assert isinstance(match["bank_content_id"], int)
                        assert isinstance(match["distance"], str)


def test_topk_lookup_response_format(client: FlaskClient):
    """Test that topk_lookup returns the correct response format."""
    # Mock the index with query_topk_index method
    mock_index = Mock()
    mock_index.query_topk_index.return_value = [
        Mock(metadata=1, similarity_info=Mock(pretty_str=lambda: "0.5")),
        Mock(metadata=2, similarity_info=Mock(pretty_str=lambda: "0.8")),
    ]
    
    with patch('OpenMediaMatch.blueprints.matching._get_index', return_value=mock_index):
        with patch('OpenMediaMatch.blueprints.matching._validate_and_transform_signal_type'):
            with patch('OpenMediaMatch.blueprints.matching.get_storage') as mock_storage:
                # Mock bank content
                mock_content1 = Mock(id=1, enabled=True, bank=Mock(name="BANK_A"))
                mock_content2 = Mock(id=2, enabled=True, bank=Mock(name="BANK_B"))
                mock_storage.return_value.bank_content_get.return_value = [mock_content1, mock_content2]
                
                response = client.get("/m/topk_lookup?signal=test_hash&signal_type=clip&k=5")
                
                assert response.status_code == 200
                results = response.json
                
                # Check response format matches TMatchByBank
                assert isinstance(results, dict)
                for bank_name, matches in results.items():
                    assert isinstance(bank_name, str)
                    assert isinstance(matches, list)
                    for match in matches:
                        assert "bank_content_id" in match
                        assert "distance" in match
                        assert isinstance(match["bank_content_id"], int)
                        assert isinstance(match["distance"], str) 