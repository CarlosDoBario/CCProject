import os
import sys

# Ensure project src is on sys.path so tests can import modules without
# requiring PYTHONPATH to be set externally. conftest.py lives in src/tests,
# so one parent up is the project src directory.
SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if SRC not in sys.path:
    sys.path.insert(0, SRC)