"""
Services module for Databricks SQL Profiler Web Interface.

Authentication strategy:
- SP (Service Principal): All SQL and REST API calls use the app's SP
  credentials via Databricks SDK.  deploy.sh grants the SP all required
  permissions (WH CAN_USE, catalog/schema CRUD, job CAN_MANAGE_RUN).
"""

import logging

from .table_writer import TableWriter, TableWriterConfig

__all__ = ["TableWriter", "TableWriterConfig"]

_logger = logging.getLogger(__name__)


def get_sp_sql_connection(http_path: str):
    """Create a SQL connection using SP credentials.

    Uses Databricks SDK Config() for authentication.
    """
    import os

    from databricks import sql as dbsql
    from databricks.sdk.core import Config

    cfg = Config()
    host = os.environ.get("DATABRICKS_HOST", "")
    if not host:
        host = cfg.host or ""
    host = host.replace("https://", "").replace("http://", "").rstrip("/")

    _logger.debug("SQL connection: SP auth, host=%s", host)
    return dbsql.connect(
        server_hostname=host,
        http_path=http_path,
        credentials_provider=_sdk_credentials_provider(cfg),
    )


def get_sp_http_headers() -> dict[str, str]:
    """Get HTTP headers for REST API calls (Genie, etc.) using SP auth."""
    from databricks.sdk.core import Config

    cfg = Config()
    headers = cfg.authenticate()
    if callable(headers):
        headers = headers()
    headers["Content-Type"] = "application/json"
    return headers


# ── Backward compatibility aliases ────────────────────────────────────────
# These will be removed in a future version.
def get_obo_sql_connection(http_path: str, user_token: str = ""):
    """Deprecated: use get_sp_sql_connection() instead."""
    return get_sp_sql_connection(http_path)


def get_obo_http_headers(user_token: str = "") -> dict[str, str]:
    """Deprecated: use get_sp_http_headers() instead."""
    return get_sp_http_headers()


def _sdk_credentials_provider(sdk_config):
    """Wrap Databricks SDK Config into a credentials_provider for dbsql.connect.

    The databricks-sql-connector expects credentials_provider to be a callable
    that returns a HeaderFactory (another callable returning headers dict).
    The SDK's Config.authenticate() returns headers directly (a dict), so we
    need a two-level wrapper.
    """

    class _Provider:
        def auth_type(self):
            return "databricks-sdk"

        def __call__(self, *args, **kwargs):
            # Return a header factory (callable that returns headers dict)
            def _header_factory():
                result = sdk_config.authenticate()
                if callable(result):
                    return result()
                return result

            return _header_factory

    return _Provider()
