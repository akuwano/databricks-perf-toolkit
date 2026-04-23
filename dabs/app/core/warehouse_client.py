"""
Databricks SQL Warehouse API client.

Provides functionality to retrieve SQL Warehouse information
from the Databricks REST API.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import requests  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)

# DBU per hour estimates by cluster size (for SQL Warehouses)
# Based on Databricks pricing documentation
DBU_PER_HOUR = {
    "2X-Small": 2,
    "X-Small": 4,
    "Small": 8,
    "Medium": 16,
    "Large": 32,
    "X-Large": 64,
    "2X-Large": 128,
    "3X-Large": 256,
    "4X-Large": 512,
}


@dataclass
class WarehouseInfo:
    """Information about a Databricks SQL Warehouse."""

    warehouse_id: str = ""
    name: str = ""
    cluster_size: str = ""  # "Small", "Medium", "Large", "X-Large", "2X-Large", etc.
    min_num_clusters: int = 0
    max_num_clusters: int = 0
    auto_stop_mins: int = 0
    spot_instance_policy: str = ""  # "COST_OPTIMIZED", "RELIABILITY_OPTIMIZED"
    enable_serverless_compute: bool = False
    warehouse_type: str = ""  # "CLASSIC", "PRO", "TYPE_UNSPECIFIED"
    channel_name: str = ""  # "CHANNEL_NAME_CURRENT", "CHANNEL_NAME_PREVIEW"
    dbsql_version: str = ""
    state: str = ""  # "RUNNING", "STOPPED", "STARTING", etc.
    num_active_sessions: int = 0
    num_clusters: int = 0
    creator_name: str = ""
    tags: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> WarehouseInfo:
        """Create WarehouseInfo from Databricks API response.

        Args:
            data: API response from GET /api/2.0/sql/warehouses/{id}

        Returns:
            WarehouseInfo instance
        """
        channel = data.get("channel", {}) or {}
        tags_list = data.get("tags", {}).get("custom_tags", []) or []
        tags = {tag.get("key", ""): tag.get("value", "") for tag in tags_list}

        return cls(
            warehouse_id=data.get("id", ""),
            name=data.get("name", ""),
            cluster_size=data.get("cluster_size", ""),
            min_num_clusters=data.get("min_num_clusters", 0),
            max_num_clusters=data.get("max_num_clusters", 0),
            auto_stop_mins=data.get("auto_stop_mins", 0),
            spot_instance_policy=data.get("spot_instance_policy", ""),
            enable_serverless_compute=data.get("enable_serverless_compute", False),
            warehouse_type=data.get("warehouse_type", ""),
            channel_name=channel.get("name", ""),
            dbsql_version=channel.get("dbsql_version", ""),
            state=data.get("state", ""),
            num_active_sessions=data.get("num_active_sessions", 0),
            num_clusters=data.get("num_clusters", 0),
            creator_name=data.get("creator_name", ""),
            tags=tags,
        )

    @property
    def size_description(self) -> str:
        """Human-readable size description.

        Returns:
            String like "Medium (1-4 clusters)"
        """
        if self.enable_serverless_compute:
            return f"{self.cluster_size} (Serverless)"
        if self.min_num_clusters == self.max_num_clusters:
            return f"{self.cluster_size} ({self.max_num_clusters} cluster{'s' if self.max_num_clusters > 1 else ''})"
        return f"{self.cluster_size} ({self.min_num_clusters}-{self.max_num_clusters} clusters)"

    @property
    def estimated_dbu_per_hour(self) -> int:
        """Estimate DBU per hour at max capacity.

        Returns:
            Estimated DBU/hour based on cluster size and max clusters
        """
        base_dbu = DBU_PER_HOUR.get(self.cluster_size, 16)  # Default to Medium
        return base_dbu * max(self.max_num_clusters, 1)

    @property
    def is_serverless(self) -> bool:
        """Check if this is a serverless warehouse."""
        return self.enable_serverless_compute

    @property
    def is_pro(self) -> bool:
        """Check if this is a Pro warehouse."""
        return self.warehouse_type == "PRO"


class WarehouseClient:
    """Client for Databricks SQL Warehouses API."""

    def __init__(self, host: str | None, token: str | None):
        """Initialize the warehouse client.

        Args:
            host: Databricks workspace URL (e.g., https://xxx.cloud.databricks.com)
            token: Databricks personal access token
        """
        self._host = host.rstrip("/") if host else None
        self._token = token
        self._cache: dict[str, WarehouseInfo] = {}

    def is_available(self) -> bool:
        """Check if the client has valid credentials.

        Returns:
            True if host and token are configured
        """
        return bool(self._host and self._token)

    def get_warehouse(self, warehouse_id: str) -> WarehouseInfo | None:
        """Get warehouse information by ID.

        Args:
            warehouse_id: The SQL Warehouse ID (endpointId)

        Returns:
            WarehouseInfo if found, None otherwise
        """
        if not self.is_available():
            logger.debug("Warehouse client not available (no credentials)")
            return None

        if not warehouse_id:
            return None

        # Check cache first
        if warehouse_id in self._cache:
            return self._cache[warehouse_id]

        try:
            url = f"{self._host}/api/2.0/sql/warehouses/{warehouse_id}"
            headers = {
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
            }
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                info = WarehouseInfo.from_api_response(response.json())
                self._cache[warehouse_id] = info
                logger.info(f"Retrieved warehouse info: {info.name} ({info.size_description})")
                return info
            elif response.status_code == 404:
                logger.warning(f"Warehouse not found: {warehouse_id}")
                return None
            else:
                logger.warning(
                    f"Failed to get warehouse {warehouse_id}: HTTP {response.status_code}"
                )
                return None

        except requests.RequestException as e:
            logger.warning(f"Error fetching warehouse {warehouse_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching warehouse {warehouse_id}: {e}")
            return None

    def list_warehouses(self) -> list[WarehouseInfo]:
        """List all SQL Warehouses in the workspace.

        Returns:
            List of WarehouseInfo objects
        """
        if not self.is_available():
            return []

        try:
            url = f"{self._host}/api/2.0/sql/warehouses"
            headers = {
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
            }
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                data = response.json()
                warehouses = []
                for item in data.get("warehouses", []):
                    info = WarehouseInfo.from_api_response(item)
                    self._cache[info.warehouse_id] = info
                    warehouses.append(info)
                return warehouses
            else:
                logger.warning(f"Failed to list warehouses: HTTP {response.status_code}")
                return []

        except Exception as e:
            logger.error(f"Error listing warehouses: {e}")
            return []

    def clear_cache(self) -> None:
        """Clear the warehouse info cache."""
        self._cache.clear()
