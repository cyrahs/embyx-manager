import sys
from pathlib import Path

TOOLS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TOOLS / 'parity'))
sys.path.insert(0, str(TOOLS / 'level2'))
