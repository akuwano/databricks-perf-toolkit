#!/usr/bin/env python3
"""
Validate consistency between key_mapping.yaml and extractors.py.

This script checks:
1. Keys referenced in extractors.py exist in key_mapping.yaml
2. Keys in JSON samples are documented in key_mapping.yaml
3. Reports any undocumented keys found in samples

Usage:
    python scripts/validate_key_mapping.py
    python scripts/validate_key_mapping.py --strict  # Exit with error on warnings
"""

import argparse
import json
import re
import sys
from pathlib import Path

import yaml


def load_key_mapping(yaml_path: Path) -> dict:
    """Load key_mapping.yaml and extract all documented paths."""
    with open(yaml_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    documented_paths = set()
    if "mappings" in data:
        for category, items in data["mappings"].items():
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict) and "path" in item:
                        path = item["path"]
                        # Normalize path (remove wildcards for comparison)
                        documented_paths.add(path)
                        # Also add parent paths
                        parts = path.split(".")
                        for i in range(1, len(parts)):
                            documented_paths.add(".".join(parts[:i]))

    return {"data": data, "paths": documented_paths}


def extract_keys_from_extractors(extractor_path: Path) -> set:
    """Extract key references from extractors.py."""
    with open(extractor_path, encoding="utf-8") as f:
        content = f.read()

    keys = set()

    # Match .get("key") patterns
    get_pattern = r'\.get\(["\']([^"\']+)["\']'
    for match in re.finditer(get_pattern, content):
        keys.add(match.group(1))

    # Match ["key"] patterns
    bracket_pattern = r'\[["\']([\w]+)["\']\]'
    for match in re.finditer(bracket_pattern, content):
        keys.add(match.group(1))

    # Match label == "value" patterns (for metrics labels)
    label_pattern = r'label\s*==\s*["\']([^"\']+)["\']'
    for match in re.finditer(label_pattern, content):
        keys.add(f"label:{match.group(1)}")

    # Match key == "value" patterns (for metadata keys)
    key_pattern = r'key\s*==\s*["\']([^"\']+)["\']'
    for match in re.finditer(key_pattern, content):
        keys.add(f"metadata_key:{match.group(1)}")

    return keys


def collect_json_paths(json_dir: Path) -> set:
    """Collect all unique JSON paths from sample files."""

    def walk(obj: any, path: str, paths: set) -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                p = f"{path}.{k}" if path else k
                paths.add(p)
                walk(v, p, paths)
        elif isinstance(obj, list):
            p = f"{path}[]" if path else "[]"
            paths.add(p)
            for item in obj:
                walk(item, p, paths)

    all_paths = set()
    json_files = list(json_dir.glob("**/*.json"))

    for json_file in json_files:
        try:
            with open(json_file, encoding="utf-8") as f:
                data = json.load(f)
            paths = set()
            walk(data, "", paths)
            all_paths |= paths
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Could not parse {json_file}: {e}", file=sys.stderr)

    return all_paths


def normalize_path(path: str) -> str:
    """Normalize a JSON path for comparison."""
    # Remove array indices and normalize
    return re.sub(r"\[\d+\]", "[]", path)


def check_documented_paths(
    json_paths: set, documented_paths: set
) -> tuple[set, set]:
    """Check which JSON paths are documented or undocumented."""
    documented = set()
    undocumented = set()

    for path in json_paths:
        normalized = normalize_path(path)
        # Check if this path or any parent is documented
        is_documented = False
        parts = normalized.split(".")
        for i in range(len(parts), 0, -1):
            check_path = ".".join(parts[:i])
            if check_path in documented_paths:
                is_documented = True
                break
            # Also check with array notation
            if check_path.replace("[]", "") in documented_paths:
                is_documented = True
                break

        if is_documented:
            documented.add(path)
        else:
            # Skip __typename as it's GraphQL internal
            if "__typename" not in path:
                undocumented.add(path)

    return documented, undocumented


def main():
    parser = argparse.ArgumentParser(
        description="Validate key_mapping.yaml consistency"
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with error code on warnings",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show detailed output",
    )
    args = parser.parse_args()

    # Determine paths
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent
    yaml_path = repo_root / "dabs" / "app" / "core" / "key_mapping.yaml"
    extractor_path = repo_root / "dabs" / "app" / "core" / "extractors.py"
    json_dir = repo_root / "json"

    errors = []
    warnings = []

    # Check if required files exist
    if not yaml_path.exists():
        errors.append(f"key_mapping.yaml not found at {yaml_path}")
        print(f"ERROR: {errors[-1]}", file=sys.stderr)
        sys.exit(1)

    if not extractor_path.exists():
        errors.append(f"extractors.py not found at {extractor_path}")
        print(f"ERROR: {errors[-1]}", file=sys.stderr)
        sys.exit(1)

    print("=" * 60)
    print("Key Mapping Validation")
    print("=" * 60)

    # Load key mapping
    print("\n[1/3] Loading key_mapping.yaml...")
    mapping = load_key_mapping(yaml_path)
    documented_paths = mapping["paths"]
    print(f"      Found {len(documented_paths)} documented paths")

    # Extract keys from extractors.py
    print("\n[2/3] Analyzing extractors.py...")
    extractor_keys = extract_keys_from_extractors(extractor_path)
    print(f"      Found {len(extractor_keys)} key references")

    # Collect JSON paths from samples
    print("\n[3/3] Scanning JSON samples...")
    if json_dir.exists():
        json_paths = collect_json_paths(json_dir)
        print(f"      Found {len(json_paths)} unique paths in samples")

        # Check documentation coverage
        documented, undocumented = check_documented_paths(
            json_paths, documented_paths
        )

        coverage = len(documented) / len(json_paths) * 100 if json_paths else 100
        print(f"\n      Documentation coverage: {coverage:.1f}%")

        if undocumented and args.verbose:
            print(f"\n      Undocumented paths ({len(undocumented)}):")
            for path in sorted(undocumented)[:20]:
                print(f"        - {path}")
            if len(undocumented) > 20:
                print(f"        ... and {len(undocumented) - 20} more")

        if len(undocumented) > 50:
            warnings.append(
                f"{len(undocumented)} JSON paths are not documented in key_mapping.yaml"
            )
    else:
        print(f"      Warning: JSON sample directory not found at {json_dir}")
        warnings.append("JSON sample directory not found")

    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)

    if errors:
        print(f"\nERRORS: {len(errors)}")
        for error in errors:
            print(f"  - {error}")

    if warnings:
        print(f"\nWARNINGS: {len(warnings)}")
        for warning in warnings:
            print(f"  - {warning}")

    if not errors and not warnings:
        print("\nAll checks passed!")

    # Exit code
    if errors:
        sys.exit(1)
    if warnings and args.strict:
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
