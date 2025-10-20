import re
from datetime import datetime, timedelta
import pandas as pd

def detect_week_bounds(now: datetime):
    # ISO week: Monday start
    start = now - timedelta(days=now.weekday())
    start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=6, hours=23, minutes=59, seconds=59)
    return start, end

def week_bounds_from_weeknum(year: int, week_num: int):
    # ISO week: Monday is 1
    first = pd.Timestamp.fromisocalendar(year, week_num, 1)
    start = first.to_pydatetime().replace(hour=0, minute=0, second=0, microsecond=0)
    end = (first + pd.Timedelta(days=6)).to_pydatetime().replace(hour=23, minute=59, second=59, microsecond=0)
    return start, end

def try_week_from_path(path: str, default_dt: datetime):
    """
    Accepts patterns like: .../week_37, .../Week37, .../week37-2025, .../2025_week_37
    Returns (week_start, week_end) or None if it can't parse.
    """
    m = re.search(r"(?:^|/)(?:week[_\s-]?)(\d{1,2})(?:[-_/](\d{4}))?(?:/|$)", path, re.IGNORECASE)
    if not m:
        return None
    week_num = int(m.group(1))
    year = int(m.group(2)) if m.group(2) else default_dt.isocalendar()[0]
    return week_bounds_from_weeknum(year, week_num)