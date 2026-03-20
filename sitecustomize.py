import os
import sys
from pathlib import Path


def _add_user_site_packages() -> None:
    version_dir = f"Python{sys.version_info.major}{sys.version_info.minor}"
    appdata = os.environ.get("APPDATA")
    if not appdata:
        return

    user_site = (
        Path(appdata)
        / "Python"
        / version_dir
        / "site-packages"
    )
    if user_site.exists():
        user_site_str = str(user_site)
        if user_site_str not in sys.path:
            sys.path.append(user_site_str)


_add_user_site_packages()
