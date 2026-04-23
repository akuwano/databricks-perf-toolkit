"""Enable running eval as: python -m eval"""

import sys
from pathlib import Path

# Add dabs/app to sys.path so core.* imports work
_app_dir = str(Path(__file__).resolve().parent.parent / "dabs" / "app")
if _app_dir not in sys.path:
    sys.path.insert(0, _app_dir)

# Add eval parent to sys.path so eval.* imports work
_eval_parent = str(Path(__file__).resolve().parent.parent)
if _eval_parent not in sys.path:
    sys.path.insert(0, _eval_parent)

from eval.cli import main

main()
