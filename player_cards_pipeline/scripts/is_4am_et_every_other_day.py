#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo


def main() -> None:
    now_et = datetime.now(ZoneInfo("America/New_York"))
    # Run window: 4:00 hour ET only, every other calendar day.
    should = (now_et.hour == 4) and ((now_et.toordinal() % 2) == 0)
    print("true" if should else "false")


if __name__ == "__main__":
    main()
