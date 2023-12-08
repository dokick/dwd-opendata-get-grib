"""Adding ./src/ directory to the python path so pytest finds the packages"""

import os.path
import sys

sys.path.insert(0, os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "src")))
