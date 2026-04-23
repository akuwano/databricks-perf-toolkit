"""Tests for Databricks SQL Warehouse client."""

from unittest.mock import Mock, patch

from core.warehouse_client import WarehouseClient, WarehouseInfo


class TestWarehouseInfo:
    """Tests for WarehouseInfo dataclass."""

    def test_warehouse_info_defaults(self):
        """Test default values."""
        info = WarehouseInfo()
        assert info.warehouse_id == ""
        assert info.name == ""
        assert info.cluster_size == ""
        assert info.min_num_clusters == 0
        assert info.max_num_clusters == 0
        assert info.enable_serverless_compute is False

    def test_warehouse_info_from_api_response(self):
        """Test creating WarehouseInfo from API response."""
        api_response = {
            "id": "abc123",
            "name": "My Warehouse",
            "cluster_size": "Medium",
            "min_num_clusters": 1,
            "max_num_clusters": 4,
            "spot_instance_policy": "COST_OPTIMIZED",
            "enable_serverless_compute": True,
            "warehouse_type": "PRO",
            "channel": {"name": "CHANNEL_NAME_CURRENT", "dbsql_version": "2025.35"},
        }
        info = WarehouseInfo.from_api_response(api_response)

        assert info.warehouse_id == "abc123"
        assert info.name == "My Warehouse"
        assert info.cluster_size == "Medium"
        assert info.min_num_clusters == 1
        assert info.max_num_clusters == 4
        assert info.spot_instance_policy == "COST_OPTIMIZED"
        assert info.enable_serverless_compute is True
        assert info.warehouse_type == "PRO"
        assert info.channel_name == "CHANNEL_NAME_CURRENT"
        assert info.dbsql_version == "2025.35"

    def test_size_description(self):
        """Test size_description property."""
        info = WarehouseInfo(
            cluster_size="Large",
            min_num_clusters=1,
            max_num_clusters=4,
        )
        assert "Large" in info.size_description
        assert "1-4" in info.size_description

    def test_estimated_dbu_per_hour(self):
        """Test DBU estimation for different sizes."""
        # Small = 8 DBU/hour base
        small = WarehouseInfo(cluster_size="Small", max_num_clusters=1)
        assert small.estimated_dbu_per_hour == 8

        # Medium = 16 DBU/hour base
        medium = WarehouseInfo(cluster_size="Medium", max_num_clusters=2)
        assert medium.estimated_dbu_per_hour == 32  # 16 * 2

        # Large = 32 DBU/hour base
        large = WarehouseInfo(cluster_size="Large", max_num_clusters=1)
        assert large.estimated_dbu_per_hour == 32


class TestWarehouseClient:
    """Tests for WarehouseClient."""

    def test_init_without_credentials(self):
        """Test initialization without credentials."""
        client = WarehouseClient(host=None, token=None)
        assert client.is_available() is False

    def test_init_with_credentials(self):
        """Test initialization with credentials."""
        client = WarehouseClient(
            host="https://test.cloud.databricks.com",
            token="test-token",
        )
        assert client.is_available() is True

    @patch("core.warehouse_client.requests.get")
    def test_get_warehouse_success(self, mock_get):
        """Test successful warehouse retrieval."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "id": "abc123",
            "name": "Test Warehouse",
            "cluster_size": "Medium",
            "min_num_clusters": 1,
            "max_num_clusters": 2,
            "warehouse_type": "PRO",
            "enable_serverless_compute": False,
        }
        mock_get.return_value = mock_response

        client = WarehouseClient(
            host="https://test.cloud.databricks.com",
            token="test-token",
        )
        info = client.get_warehouse("abc123")

        assert info is not None
        assert info.warehouse_id == "abc123"
        assert info.name == "Test Warehouse"
        assert info.cluster_size == "Medium"

    @patch("core.warehouse_client.requests.get")
    def test_get_warehouse_not_found(self, mock_get):
        """Test warehouse not found."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        client = WarehouseClient(
            host="https://test.cloud.databricks.com",
            token="test-token",
        )
        info = client.get_warehouse("nonexistent")

        assert info is None

    @patch("core.warehouse_client.requests.get")
    def test_get_warehouse_api_error(self, mock_get):
        """Test API error handling."""
        mock_get.side_effect = Exception("Connection error")

        client = WarehouseClient(
            host="https://test.cloud.databricks.com",
            token="test-token",
        )
        info = client.get_warehouse("abc123")

        assert info is None

    def test_get_warehouse_without_credentials(self):
        """Test get_warehouse without credentials returns None."""
        client = WarehouseClient(host=None, token=None)
        info = client.get_warehouse("abc123")
        assert info is None
