# app/utils/time_utils.py
from datetime import datetime
import pandas as pd
import pytz

def is_today_sh(day_str: str) -> bool:
    if not day_str: return False
    try:
        tz = pytz.timezone("Asia/Shanghai")
        return pd.to_datetime(day_str).date() == datetime.now(tz).date()
    except Exception:
        return False
