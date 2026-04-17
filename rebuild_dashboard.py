#!/usr/bin/env python3
"""
rebuild_dashboard.py — Sandbox-portable wrapper around update_dashboard.py

Sets the BASE path dynamically (update_dashboard.py has it hardcoded to
/Users/dlimberopoulos/...) and invokes its main transform, then verifies
the output HTML was written.
"""

import os
import sys
import importlib.util
import shutil
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))


def patch_and_run():
    # Load update_dashboard.py as a module
    spec = importlib.util.spec_from_file_location(
        "update_dashboard", os.path.join(HERE, "update_dashboard.py")
    )
    ud = importlib.util.module_from_spec(spec)
    # Override BASE before executing the module
    # Easier: read source, substitute BASE, exec
    with open(os.path.join(HERE, "update_dashboard.py")) as f:
        src = f.read()

    # Inject dynamic BASE at the top
    patched = src.replace(
        'BASE = "/Users/dlimberopoulos/Documents/Womens jeans scraper"',
        f'BASE = "{HERE}"',
    )

    # Run under current globals
    ns = {"__name__": "__main__", "__file__": os.path.join(HERE, "update_dashboard.py")}
    exec(compile(patched, os.path.join(HERE, "update_dashboard.py"), "exec"), ns)


def verify_outputs():
    dashboard = os.path.join(HERE, "cross_retailer_dashboard_v2.html")
    if not os.path.exists(dashboard):
        print("ERROR: dashboard not written")
        return False
    size = os.path.getsize(dashboard)
    print(f"\ncross_retailer_dashboard_v2.html: {size:,} bytes")
    # Basic sanity: should contain all 9 groups
    with open(dashboard) as f:
        html = f.read()
    import re
    groups = set(re.findall(r'"g"\s*:\s*"([^"]+)"', html))
    print(f"Retailer groups in dashboard: {sorted(groups)}")
    return True


if __name__ == "__main__":
    # Backup existing dashboard
    dashboard = os.path.join(HERE, "cross_retailer_dashboard_v2.html")
    if os.path.exists(dashboard):
        bak = f"{dashboard}.bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        shutil.copy2(dashboard, bak)
        print(f"Backed up existing dashboard to {bak}")
    patch_and_run()
    verify_outputs()
