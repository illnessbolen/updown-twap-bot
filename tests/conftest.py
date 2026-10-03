import sys
from pathlib import Path

# test_recorder импортирует фейковый сервер из test_feed
sys.path.insert(0, str(Path(__file__).resolve().parent))
