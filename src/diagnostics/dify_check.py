from __future__ import annotations

import json

from src.dify.client import check_dify_health


if __name__ == "__main__":
    print(json.dumps(check_dify_health(), ensure_ascii=False, indent=2))
