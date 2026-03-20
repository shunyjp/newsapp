import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def _load_dotenv(dotenv_path: Path) -> None:
    if not dotenv_path.exists():
        return

    for line in dotenv_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue

        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


_load_dotenv(BASE_DIR / ".env")

OLLAMA_MODEL = "llama3.2:3b"
CHUNK_SIZE = 2200
OVERLAP_RATIO = 0.10
DB_PATH = str(DATA_DIR / "news_intelligence.db")
OLLAMA_URL = "http://localhost:11434/api/generate"
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY", "").strip()
YOUTUBE_API_BASE_URL = "https://www.googleapis.com/youtube/v3"
REQUEST_TIMEOUT = 30
OLLAMA_TIMEOUT = 120
MAX_VIDEOS = 5
SUMMARY_RETRIES = 3
VIDEO_WORKERS = 2
CHUNK_WORKERS = 2
