"""
Lightweight CLI health check for the Telegram automation system.

Read-only: never creates, migrates, or modifies the database.

Usage:
    python healthcheck.py
"""

from __future__ import annotations

import json
import sys

from database import get_health_report


def is_healthy(report):
    """
    Overall health requires database + listener + scheduler all OK.

    Unknown or stale heartbeats are not healthy.
    """
    return bool(
        report.get("database_accessible")
        and report.get("integrity") == "ok"
        and report.get("error") is None
        and report.get("schema_ok") is True
        and report.get("listener_status") == "ok"
        and report.get("scheduler_status") == "ok"
    )


def main():
    report = get_health_report()
    report["overall_healthy"] = is_healthy(report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["overall_healthy"] else 1


if __name__ == "__main__":
    sys.exit(main())
