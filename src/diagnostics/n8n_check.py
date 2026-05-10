from __future__ import annotations

import json

from src.n8n.client import check_n8n_health


if __name__ == "__main__":
    print(json.dumps(check_n8n_health(), ensure_ascii=False, indent=2))
