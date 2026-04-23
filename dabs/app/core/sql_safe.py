"""SQL identifier validation to prevent injection in table/schema/catalog names."""

from __future__ import annotations

import re

# Allow only alphanumeric + underscore + hyphen (common in Databricks identifiers)
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z0-9_\-]+$")


def validate_identifier(name: str, label: str = "identifier") -> str:
    """Validate that a string is a safe SQL identifier.

    Raises ValueError if the name contains characters that could enable
    SQL injection when used in table/schema/catalog names.

    Args:
        name: The identifier to validate.
        label: Human-readable label for error messages.

    Returns:
        The validated identifier (unchanged).
    """
    if not name:
        raise ValueError(f"Empty {label}")
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Invalid {label}: {name!r}. "
            f"Only alphanumeric characters, underscores, and hyphens are allowed."
        )
    return name


def safe_fqn(catalog: str, schema: str, table_name: str) -> str:
    """Build a fully-qualified table name with validation.

    Args:
        catalog: Catalog name.
        schema: Schema name.
        table_name: Table name (may include prefix).

    Returns:
        Validated fully-qualified name: ``catalog.schema.table_name``
    """
    validate_identifier(catalog, "catalog")
    validate_identifier(schema, "schema")
    validate_identifier(table_name, "table name")
    return f"{catalog}.{schema}.{table_name}"
