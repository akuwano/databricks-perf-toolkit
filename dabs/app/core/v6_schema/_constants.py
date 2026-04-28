"""Shared constants for the v6_schema package.

Kept tiny and dependency-free so any submodule (aliases, enrich
pipeline, normalizer, tests) can import without triggering cycles.
"""

# Canonical Report schema version. Bump in lockstep with
# ``schemas/report_v6.schema.json`` whenever the schema's wire format
# changes (renames, type changes, removals).
SCHEMA_VERSION = "v6.0"
