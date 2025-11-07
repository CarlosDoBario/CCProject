import os
import sys

# Ensure project src is on sys.path so tests can import modules without setting PYTHONPATH externally.
# conftest lives in project/src/tests, so one parent up is project/src
SRC = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if SRC not in sys.path:
    sys.path.insert(0, SRC)