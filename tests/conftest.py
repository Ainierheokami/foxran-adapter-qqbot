"""Load this source checkout against an isolated host runtime for plugin tests."""
import importlib.util
import logging
import os
from pathlib import Path
import sys
import tempfile

host = os.environ.get("FOXRAN_HOST_ROOT")
if host:
    sys.path.insert(0, host)
for name in ("DB", "CONFIG", "LOG"):
    os.environ.setdefault(f"FOXRAN_{name}_DIR", tempfile.mkdtemp(prefix=f"qqbot-test-{name.lower()}-"))

# The adapter uses its canonical package name even in a separate checkout.
import app.adapters
root = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("app.adapters.qqbot", root / "__init__.py", submodule_search_locations=[str(root)])
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
setattr(app.adapters, "qqbot", module)
spec.loader.exec_module(module)


def pytest_unconfigure(config):
    logging.raiseExceptions = False
