# app/utils/cache.py
import hashlib, pickle, zlib, time
import pandas as pd
from flask import current_app
from .time_utils import is_today_sh
from ..extensions import rds

def df_pack(df) -> bytes:
    return zlib.compress(pickle.dumps(df, protocol=5), 6)

def df_unpack(b: bytes):
    try:
        return pickle.loads(zlib.decompress(b))
    except Exception:
        return pd.DataFrame()

def build_key(prefix: str, params: dict, include: list) -> str:
    picked = {k: str(params.get(k, "")) for k in include}
    plain = "|".join(f"{k}={picked[k]}" for k in sorted(picked))
    digest = hashlib.md5(plain.encode("utf-8")).hexdigest()
    suffix = ":".join([picked.get("product","").upper(),
                       picked.get("trading_day",""), picked.get("sessions","")])
    return f"ta:{current_app.config['CACHE_VER']}:{prefix}:{suffix}:{digest}"

def ttl_by_trading_day(day_str: str, hot_ttl=30, cold_ttl=24*3600) -> int:
    return hot_ttl if is_today_sh(day_str) else cold_ttl

def cache_get_bytes(key: str):
    try: return rds.get(key)
    except Exception: return None

def cache_set_bytes(key: str, data: bytes, ttl: int):
    try: rds.setex(key, ttl, data)
    except Exception: pass

class RedisLock:
    def __init__(self, key: str, ex: int = 30):
        self.key = f"{key}:lock"; self.ex = ex; self.acquired=False
    def __enter__(self):
        try: self.acquired = bool(rds.set(self.key, b"1", nx=True, ex=self.ex))
        except Exception: self.acquired=False
        return self.acquired
    def __exit__(self, exc_type, exc, tb):
        try:
            if self.acquired: rds.delete(self.key)
        except Exception:
            pass

def spin_wait_for_cache(key: str, wait_ms=100, tries=50):
    for _ in range(tries):
        b = cache_get_bytes(key)
        if b: return b
        time.sleep(wait_ms/1000)
    return None
