"""Test configuration for eval tests."""

import sys
from pathlib import Path

# Add dabs/app to sys.path
_app_dir = str(Path(__file__).resolve().parent.parent.parent / "dabs" / "app")
if _app_dir not in sys.path:
    sys.path.insert(0, _app_dir)

# Add project root to sys.path
_root = str(Path(__file__).resolve().parent.parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)
