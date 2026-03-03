# app.py
# ⚠️ 此文件已废弃，建议使用 app/__init__.py 的 create_app() 和 run.py 启动
# 保留此文件仅用于向后兼容，新代码请使用模块化结构
#
# 新的代码结构：
# - app/services/data_loader.py - 数据加载
# - app/services/calculations.py - 计算功能
# - app/services/contract_utils.py - 合约工具
# - app/blueprints/ - 路由模块
#
# 启动方式：python backend/run.py

# app.py —— Flask + SQLAlchemy + MySQL + JWT（与前端接口保持一致）
from datetime import datetime, timedelta
import json
import jwt
from flask import Flask, request, jsonify
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
import pytz
from datetime import timedelta, date
from tools.timescaleManager import TimescaleCRUD
from models.models import ChinaFuturesL1TABar, VChinaFuturesDaybar, ChinaFuturesBaseInfo
from config.settings import DATABASE_URL, TRADING_SESSION_RULES  # 你已有这些配置
from common.logger import systemLogger
from typing import Optional, Tuple, List, Dict
import numpy as np
import time
from tradeAssistantSpread import (
    load_main_sub_data, get_tick_size, remove_open_close_noise,
    align_main_sub_minute, filter_spread_with_limit,
    compute_leg_return_series, compute_leg_volatility,
    compute_spread_volatility, compute_spread_leg_correlation,
    _is_main_near, load_pair_data
)
from models.models import (
    ChinaTradingDay, VChinaFuturesDaybar, ChinaFuturesL1TABar, ChinaFuturesBaseInfo
)
from config.settings import DATABASE_URL, TRADING_SESSION_RULES, TRADING_Open_SESSION_RULES
from sqlalchemy import and_, func, desc
from collections import defaultdict
import re

# ===== Redis 缓存基建（新增）=====
import os, hashlib, time
import redis
try:
    import orjson as _json
    def _jdumps(obj): return _json.dumps(obj)
    def _jloads(b): return _json.loads(b)
except Exception:
    import json as _json
    def _jdumps(obj): return _json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    def _jloads(b): return _json.loads(b.decode("utf-8"))


import pickle, zlib
def _df_pack(df) -> bytes:
    # protocol=5 性能/体积较好；压缩等级 6 折中
    return zlib.compress(pickle.dumps(df, protocol=5), 6)

def _df_unpack(b: bytes):
    try:
        return pickle.loads(zlib.decompress(b))
    except Exception:
        return pd.DataFrame()


REDIS_URL = os.getenv("REDIS_URL", "redis://:avg123@localhost:6379/0")  # 如果设置了密码：redis://:PASSWORD@localhost:6379/0
rds = redis.from_url(REDIS_URL, decode_responses=False)         # 存 bytes，不解码；性能更好
CACHE_VER = "v1"  # 版本号，升级逻辑时改一下即可整体失效

def _is_today_sh(day_str: str) -> bool:
    if not day_str: return False
    try:
        tz = pytz.timezone("Asia/Shanghai")
        return pd.to_datetime(day_str).date() == datetime.now(tz).date()
    except Exception:
        return False

def _ttl_by_trading_day(day_str: str, *, hot_ttl=30, cold_ttl=24*3600) -> int:
    """今天(交易中/刚出结果)短 ttl，历史长 ttl。默认 today=30s，history=1d"""
    return hot_ttl if _is_today_sh(day_str) else cold_ttl

def _build_key(prefix: str, params: dict, include: list) -> str:
    """稳健的 key 生成：用指定参数 + md5 摘要，避免过长"""
    picked = {k: str(params.get(k, "")) for k in include}
    plain = "|".join(f"{k}={picked[k]}" for k in sorted(picked))
    digest = hashlib.md5(plain.encode("utf-8")).hexdigest()
    # 也把几个关键字段直接拼上，便于人工排查
    suffix = ":".join([picked.get("product","").upper(),
                       picked.get("trading_day",""),
                       picked.get("sessions","")])
    return f"ta:{CACHE_VER}:{prefix}:{suffix}:{digest}"

def _cache_get_bytes(key: str):
    try:
        return rds.get(key)
    except Exception:
        return None

def _cache_set_bytes(key: str, data: bytes, ttl: int):
    try:
        rds.setex(key, ttl, data)
    except Exception:
        pass

class _Lock:
    """简单的 Redis 分布式锁，防止缓存击穿；EX 默认 30s"""
    def __init__(self, key: str, ex: int = 30):
        self.key = f"{key}:lock"
        self.ex = ex
        self.acquired = False
    def __enter__(self):
        try:
            self.acquired = bool(rds.set(self.key, b"1", nx=True, ex=self.ex))
        except Exception:
            self.acquired = False
        return self.acquired
    def __exit__(self, exc_type, exc, tb):
        try:
            if self.acquired: rds.delete(self.key)
        except Exception:
            pass

def _spin_wait_for_cache(key: str, wait_ms=100, tries=50):
    """没拿到锁时，短暂轮询等别人填充缓存。默认最多等 ~5s"""
    for _ in range(tries):
        b = _cache_get_bytes(key)
        if b: return b
        time.sleep(wait_ms/1000)
    return None




# ===== 基础配置 =====
SECRET_KEY = "change-me-to-a-random-secret"   # 生产务必改成强随机
TOKEN_EXPIRE_SECONDS = 2 * 60 * 60            # 2小时

# ===== MySQL 连接串（按你的实际账号/库名改掉下面一行）=====
DB_URI = "mysql+pymysql://ta_admin:StrongPassword123!@127.0.0.1:3306/ta_admin?charset=utf8mb4"

app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False
app.config["SQLALCHEMY_DATABASE_URI"] = DB_URI
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
# 防止长连接断开 & MySQL 空闲断开
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_pre_ping": True,
    "pool_recycle": 280
}

# === 新增：Timescale(Postgres) 连接（与 MySQL 分开）===
ts_crud = TimescaleCRUD(DATABASE_URL, pool_size=20, max_overflow=0)
SH_TZ = pytz.timezone("Asia/Shanghai")
timescale_crud = TimescaleCRUD(DATABASE_URL)


# 开发期放开 CORS（你走 devServer 代理的话，这段无伤大雅，直连也能用）
CORS(app, resources={r"/*": {"origins": "*"}},
     supports_credentials=False,
     methods=["GET", "POST", "OPTIONS"],
     allow_headers=["Content-Type", "X-Token", "Authorization"])

db = SQLAlchemy(app)

# ===== 数据模型 =====
class User(db.Model):
    __tablename__ = "users"
    id            = db.Column(db.Integer, primary_key=True)
    username      = db.Column(db.String(80), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    name          = db.Column(db.String(80), nullable=False, default="")
    avatar        = db.Column(db.String(255), nullable=False, default="")
    roles_json    = db.Column(db.Text, nullable=False, default="[]")  # JSON 字符串存角色

    @property
    def roles(self):
        try:
            return json.loads(self.roles_json or "[]")
        except Exception:
            return []

    @roles.setter
    def roles(self, value):
        self.roles_json = json.dumps(value or [])


def make_ok(data=None, message="ok"):
    return jsonify({"code": 20000, "data": data, "message": message})

def make_err(code=50000, message="Error", http_status=400):
    return jsonify({"code": code, "message": message}), http_status

def generate_token(username: str) -> str:
    now = datetime.utcnow()
    payload = {"sub": username, "iat": now, "exp": now + timedelta(seconds=TOKEN_EXPIRE_SECONDS)}
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def verify_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload, None
    except jwt.ExpiredSignatureError:
        return None, ("Token expired", 50014)
    except Exception:
        return None, ("Invalid token", 50008)

def init_db_and_seed():
    db.create_all()
    # 首次启动自动创建管理员 admin / 111111
    if not User.query.filter_by(username="admin").first():
        u = User(
            username="admin",
            name="Admin",
            avatar="",
            roles=["admin"],
            password_hash=generate_password_hash("111111")
        )
        db.session.add(u)
        db.session.commit()

def get_product_prefix(instrument_id: str) -> str:
    # 逐字符遍历，找到第一个非字母位置，然后切片
    for i, ch in enumerate(instrument_id):
        if not ch.isalpha():
            return instrument_id[:i].lower()
    return instrument_id.lower() # 如果全是字母，返回全部

def remove_close_noise(df: pd.DataFrame) -> pd.DataFrame:
    """
    仅保留“开盘 09:32–10:00”时间段内的数据（用于股指开盘窗口统计）。
    依据 TRADING_Open_SESSION_RULES 按品种过滤；若某品种未配置规则，则不做过滤（放行）。
    """
    if df.empty:
        systemLogger.warning("⚠️ 输入数据为空，跳过开盘窗口过滤")
        return df

    try:
        df = df.copy()
        df["__time_only__"] = pd.to_datetime(df["time"], errors="coerce").dt.time
        df["__product__"] = df["instrument_id"].apply(get_product_prefix)  # 小写

        def is_in_open_window(row):
            product = row["__product__"]
            t = row["__time_only__"]
            if pd.isnull(t):
                return False
            sessions = TRADING_Open_SESSION_RULES.get(product, None)
            # 若未配置规则，放行该行（不过你只在股指调用，此处更安全）
            if not sessions:
                return True
            for s in sessions:
                start = datetime.strptime(s[0], "%H:%M").time()
                end   = datetime.strptime(s[1], "%H:%M").time()
                if start <= end:
                    if start <= t <= end:
                        return True
                else:
                    # 跨午夜的情况（此处不会出现，但保持兼容）
                    if t >= start or t <= end:
                        return True
            return False

        before_rows = len(df)
        df = df[df.apply(is_in_open_window, axis=1)].copy()
        after_rows = len(df)
        systemLogger.info(f"🧹 开盘窗口过滤: 原始={before_rows} 行，保留={after_rows} 行，过滤={before_rows - after_rows} 行")

        return df.drop(columns=["__time_only__", "__product__"])

    except Exception as e:
        systemLogger.exception(f"❌ 开盘窗口过滤失败: 错误={e}")
        return df

def get_avg_volume_many_cached(timescale_crud, instruments: List[str], end_date: str, days: int = 30) -> Dict[str, int]:
    instruments = [i for i in (instruments or []) if i]
    if not instruments or not end_date:
        return {i:0 for i in instruments}

    # 先读缓存
    result, miss = {}, []
    ttl = _ttl_by_trading_day(end_date, hot_ttl=300, cold_ttl=24*3600)
    for inst in instruments:
        k = f"ta:{CACHE_VER}:avgvol:{inst.upper()}:{end_date}:{int(days)}"
        b = _cache_get_bytes(k)
        if b:
            try:
                result[inst] = int(_jloads(b)); continue
            except Exception:
                pass
        miss.append(inst)

    if not miss:
        return result

    # 一条 SQL 覆盖所有 miss
    from sqlalchemy import func
    end_d = pd.to_datetime(end_date).date()
    start_d = end_d - timedelta(days=days)
    with timescale_crud.session_scope() as s:
        subq = (
            s.query(
                ChinaFuturesL1TABar.instrument_id.label('inst'),
                ChinaFuturesL1TABar.trading_day.label('d'),
                func.sum(ChinaFuturesL1TABar.volume).label('v')
            )
            .filter(
                ChinaFuturesL1TABar.instrument_id.in_(miss),
                ChinaFuturesL1TABar.trading_day >= start_d,
                ChinaFuturesL1TABar.trading_day <= end_d
            )
            .group_by(ChinaFuturesL1TABar.instrument_id, ChinaFuturesL1TABar.trading_day)
        ).subquery()

        rows = s.query(subq.c.inst, func.avg(subq.c.v)).group_by(subq.c.inst).all()

    avg_map = {inst: 0 for inst in miss}
    for inst, avg_v in rows:
        avg_map[inst] = int(avg_v or 0)
        _cache_set_bytes(f"ta:{CACHE_VER}:avgvol:{inst.upper()}:{end_date}:{int(days)}",
                         _jdumps(avg_map[inst]), ttl)

    result.update(avg_map)
    return result

def _prev_trading_day(session, d: date) -> Optional[date]:
    """从交易日历表里拿前一交易日（含容错）。"""
    row = (session.query(ChinaTradingDay.pre_trading_day)
           .filter(ChinaTradingDay.trading_day == d)
           .first())
    return row[0] if row and row[0] else None

def _iter_trading_days(session, start_d: date, end_d: date) -> List[date]:
    rows = (session.query(ChinaTradingDay.trading_day)
            .filter(ChinaTradingDay.trading_day >= start_d,
                    ChinaTradingDay.trading_day <= end_d)
            .order_by(ChinaTradingDay.trading_day.asc())
            .all())
    return [r[0] for r in rows]

def _load_minute_df(session, inst: str, d: date) -> pd.DataFrame:
    """取某合约某自然日的分钟线（只取 close_price/volume/time）"""
    q = (session.query(
            ChinaFuturesL1TABar.time,
            ChinaFuturesL1TABar.close_price,
            ChinaFuturesL1TABar.volume
        )
        .filter(ChinaFuturesL1TABar.instrument_id == inst,
                ChinaFuturesL1TABar.trading_day == d)
        .order_by(ChinaFuturesL1TABar.time.asc())
    )
    df = pd.read_sql(q.statement, session.bind)
    if df.empty:
        return df
    # 统一时间为上海时区（若你的 time 已经是带 tz 的，可按需要调整/去掉这一行）
    if not pd.api.types.is_datetime64_any_dtype(df['time']):
        df['time'] = pd.to_datetime(df['time'])
    try:
        df['time'] = df['time'].dt.tz_convert('Asia/Shanghai')
    except Exception:
        # 如果是 naive，就先本地化
        df['time'] = pd.to_datetime(df['time']).dt.tz_localize('Asia/Shanghai')
    return df

def _filter_by_sessions(df: pd.DataFrame, product: str, sessions: str) -> pd.DataFrame:
    """按 TRADING_SESSION_RULES 过滤分钟（支持 all/day/night）。"""
    rules = TRADING_SESSION_RULES.get(product.lower(), [])
    if not rules or sessions == 'all':
        return df
    # 只取日盘或夜盘的时段
    def in_ranges(ts: pd.Timestamp, wanted: str) -> bool:
        tstr = ts.tz_convert('Asia/Shanghai').strftime('%H:%M')
        h, m = map(int, tstr.split(':'))
        minute = h * 60 + m
        ok_ranges = []
        if wanted == 'day':
            # 简单规则：把 09:00-15:00 这类白盘时段挑出来（或用你 TRADING_SESSION_RULES 中 day 段）
            ok_ranges = []
            for s, e in rules:
                if s < '18:00':  # 粗分：18点前当日
                    ok_ranges.append((s, e))
        else:  # night
            for s, e in rules:
                if s >= '18:00':  # 18点后粗分为夜盘
                    ok_ranges.append((s, e))
        # 若没配到，直接不过滤
        if not ok_ranges:
            return True
        for s, e in ok_ranges:
            if s <= tstr <= e:
                return True
        return False

    mask = df['time'].map(lambda x: in_ranges(x, sessions))
    return df.loc[mask].copy()

def _concat_prev_and_today(df_prev: pd.DataFrame, df_today: pd.DataFrame, product: str, sessions: str) -> pd.DataFrame:
    """把前一自然日 + 当日拼接，然后按 sessions 过滤。此前一日缺失时只用当日。"""
    if df_prev is None or df_prev.empty:
        out = df_today.copy()
    else:
        out = pd.concat([df_prev, df_today], ignore_index=True)
    if out.empty:
        return out
    out = _filter_by_sessions(out, product, sessions)
    return out

def _day_metrics(df_near: pd.DataFrame, df_far: pd.DataFrame) -> Optional[Dict[str, float]]:
    if df_near.empty or df_far.empty:
        return None
    a = df_near[['time', 'close_price', 'volume']].rename(columns={'close_price': 'near', 'volume': 'vol_near'})
    b = df_far [['time', 'close_price', 'volume']].rename(columns={'close_price': 'far',  'volume': 'vol_far' })
    df = pd.merge(a, b, on='time', how='inner').sort_values('time')
    if df.empty:
        return None

    m = {}
    m['vol_near'] = int(df['vol_near'].sum())
    m['vol_far']  = int(df['vol_far'].sum())
    m['dP_near']  = float(df['near'].iloc[-1] - df['near'].iloc[0])
    m['dP_far']   = float(df['far' ].iloc[-1] - df['far' ].iloc[0])
    m['volat_near'] = float(df['near'].diff().dropna().std()) if len(df) > 1 else 0.0
    m['volat_far']  = float(df['far' ].diff().dropna().std()) if len(df) > 1 else 0.0

    spread = df['near'] - df['far']
    m['spread_delta'] = float(spread.iloc[-1] - spread.iloc[0])
    m['spread_volat'] = float(spread.diff().dropna().std()) if len(spread) > 1 else 0.0

    sd = spread.diff()
    m['corr_spread_near'] = float(sd.corr(df['near'].diff())) if len(df) > 2 else float('nan')
    m['corr_spread_far']  = float(sd.corr(df['far' ].diff())) if len(df) > 2 else float('nan')
    return m

# ==== 工具：从合约代码解析 YYYYMM，用来判断近月/远月 ====
def _parse_contract_yyyymm(instr: str) -> Optional[int]:
    """如 cu2509 -> 202509；无法解析返回 None"""
    if not instr:
        return None
    # 规则：字母前缀 + 4位/5位数字（如 2509 / 25101 也有少数品种 3位/4位），此处按常见 4位处理
    import re
    m = re.search(r'(\d{4,5})$', instr)
    if not m:
        return None
    yy = m.group(1)
    # 4位：25(年)09(月)
    if len(yy) == 4:
        y = 2000 + int(yy[:2])
        mth = int(yy[2:])
        return y * 100 + mth
    # 5位：25(年)10(月=10/11/12，多位) -> 25101/25111/25121（不同交易所习惯可能不同，这里简单兜底）
    try:
        y = 2000 + int(yy[:2])
        mth = int(yy[2:])
        return y * 100 + mth
    except:
        return None

# ==== 新的选约函数（分钟线优先；daybar 为兜底；都没有则再回溯几天）====
def _pick_main_sub_by_volume_first(session, product: str, d: date) -> Tuple[Optional[str], Optional[str], str]:
    """
    选主次主力（当日成交量优先）：
      1) 在当天 d，按 china_futl1_ta_bar1 的 volume 求和，取前2名（主/次）
      2) 若不够 2 个，则回退：用 vchina_futures_daybar 的 rank_volume（同日）
      3) 若仍不够，再向前最多回溯 3 个交易日，重复 1)+2) 的逻辑
    返回: (main, sub, reason)；reason: 'minute' | 'rank' | 'minute_prev' | 'rank_prev' | 'no_data'
    说明：
      - 这里的 main/sub 指“按当日总成交量排名”的主力/次主力
      - 近月/远月请在业务里再根据到期（_parse_contract_yyyymm）去判断
    """
    # # 优先：当天分钟线
    # rows = (
    #     session.query(
    #         ChinaFuturesL1TABar.instrument_id,
    #         func.sum(ChinaFuturesL1TABar.volume).label('vol'),
    #     )
    #     .join(
    #         ChinaFuturesBaseInfo,
    #         ChinaFuturesL1TABar.instrument_id == ChinaFuturesBaseInfo.instrument_id,
    #     )
    #     .filter(
    #         ChinaFuturesL1TABar.trading_day == d,
    #         ChinaFuturesBaseInfo.product.ilike(product),
    #     )
    #     .group_by(ChinaFuturesL1TABar.instrument_id)
    #     .order_by(desc('vol'))
    #     .limit(2)
    #     .all()
    # )
    # if len(rows) >= 2:
    #     return rows[0][0], rows[1][0], 'minute'

    # 兜底：当天 daybar 排名
    r = (
        session.query(VChinaFuturesDaybar.instrument_id, VChinaFuturesDaybar.rank_volume)
        .filter(
            VChinaFuturesDaybar.product.ilike(product),
            VChinaFuturesDaybar.trading_day == d,
        )
        .order_by(VChinaFuturesDaybar.rank_volume.asc())
        .limit(2)
        .all()
    )
    # systemLogger.info(f'{product}, {d}')
    # systemLogger.info(f'pick_main_sub_by_volume_first: {r}')
    if len(r) >= 2:
        return r[0][0], r[1][0], 'rank'

    # 再回溯最多 3 天
    tries, cur = 0, _prev_trading_day(session, d)
    while cur and tries < 3:
        rows_prev = (
            session.query(
                ChinaFuturesL1TABar.instrument_id,
                func.sum(ChinaFuturesL1TABar.volume).label('vol'),
            )
            .join(
                ChinaFuturesBaseInfo,
                ChinaFuturesL1TABar.instrument_id == ChinaFuturesBaseInfo.instrument_id,
            )
            .filter(
                ChinaFuturesL1TABar.trading_day == cur,
                ChinaFuturesBaseInfo.product.ilike(product),
            )
            .group_by(ChinaFuturesL1TABar.instrument_id)
            .order_by(desc('vol'))
            .limit(2)
            .all()
        )
        if len(rows_prev) >= 2:
            return rows_prev[0][0], rows_prev[1][0], 'minute_prev'

        r_prev = (
            session.query(VChinaFuturesDaybar.instrument_id, VChinaFuturesDaybar.rank_volume)
            .filter(
                VChinaFuturesDaybar.product.ilike(product),
                VChinaFuturesDaybar.trading_day == cur,
                VChinaFuturesDaybar.rank_volume.isnot(None),
            )
            .order_by(VChinaFuturesDaybar.rank_volume.asc())
            .limit(2)
            .all()
        )
        if len(r_prev) >= 2:
            return r_prev[0][0], r_prev[1][0], 'rank_prev'

        cur = _prev_trading_day(session, cur)
        tries += 1

    return None, None, 'no_data'

def get_recent_trading_days(tradingday: str, n: int, timescale_crud: TimescaleCRUD) -> List[str]:
    """
    取包含 tradingday 在内的最近 n 个交易日，按从早到晚返回。
    """
    try:
        dt = pd.to_datetime(tradingday).date()
        with timescale_crud.session_scope() as session:
            rows = (session.query(ChinaTradingDay.trading_day)
                    .filter(ChinaTradingDay.trading_day <= dt)
                    .order_by(ChinaTradingDay.trading_day.desc())
                    .limit(n).all())
        days = [r.trading_day.strftime("%Y-%m-%d") for r in rows][::-1]
        return days
    except Exception as e:
        systemLogger.exception(f"❌ 获取最近交易日失败: tradingday={tradingday}, n={n}, 错误: {e}")
        return [tradingday]

def load_contract_data(instrument_id: str, trading_day: str, timescale_crud, previous: bool = False) -> pd.DataFrame:
    try:
        sh_tz = pytz.timezone("Asia/Shanghai")
        target_day = datetime.strptime(trading_day, "%Y-%m-%d").date()

        with timescale_crud.session_scope() as session:
            if previous:
                pre_day_obj = session.query(ChinaTradingDay.pre_trading_day)\
                    .filter(ChinaTradingDay.trading_day == target_day).first()
                if not pre_day_obj:
                    systemLogger.warning(f"❌ 未找到前一交易日: 当前交易日={trading_day}")
                    return pd.DataFrame()
                target_day = pre_day_obj.pre_trading_day
                systemLogger.info(f"使用前一交易日: 原始={trading_day}, 前一日={target_day}")

            # 更宽松的时间范围，以容纳夜盘数据
            start_dt = datetime.combine(target_day - timedelta(days=14), datetime.min.time()).astimezone(pytz.utc)
            end_dt   = datetime.combine(target_day + timedelta(days=14), datetime.min.time()).astimezone(pytz.utc)

            rows = session.query(ChinaFuturesL1TABar)\
                .filter(ChinaFuturesL1TABar.time.between(start_dt, end_dt))\
                .filter(ChinaFuturesL1TABar.trading_day == target_day)\
                .filter(ChinaFuturesL1TABar.instrument_id == instrument_id)\
                .order_by(ChinaFuturesL1TABar.time)\
                .all()

            if not rows:
                systemLogger.warning(f"⚠️ 查询分钟数据为空: 合约={instrument_id}, 日期范围={start_dt}~{end_dt}")
                return pd.DataFrame()

            df = pd.DataFrame([{
                **{col.name: getattr(row, col.name) for col in row.__table__.columns}
            } for row in rows])

            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time']).dt.tz_convert(sh_tz)

            systemLogger.info(f"✅ 加载分钟数据成功: 合约={instrument_id}, 行数={len(df)}, 日期={target_day}")
            return df

    except Exception as e:
        systemLogger.exception(f"❌ 数据库查询失败: 合约={instrument_id}, 日期={trading_day}, previous={previous}, 错误: {e}")
        return pd.DataFrame()


def build_weekly_spread_samples(
    main_info: dict,
    sub_info: dict,
    tradingday: str,
    timescale_crud: TimescaleCRUD,
    window_trading_days: int = 5
) -> Tuple[pd.Series, Tuple[str, str], Optional[np.ndarray], float]:
    """
    构建“近一周(近 n 个交易日)”的 近月-远月 方向统一的 spread 样本，并给出统一分箱 bins。
    返回: (weekly_spread, (start_day, end_day), bins, base_tick)
    - 若无可用样本，weekly_spread 为空，bins 为 None。
    """
    try:
        days = get_recent_trading_days(tradingday, window_trading_days, timescale_crud)
        if not days:
            return pd.Series(dtype="float64"), (tradingday, tradingday), None, 0.1

        weekly_parts: List[pd.Series] = []
        ticks: List[float] = []

        main_contract = main_info["instrument_id"]
        sub_contract = sub_info["instrument_id"]

        for d in days:
            # 加载当天数据
            main_df = load_contract_data(main_contract, d, timescale_crud)
            main_df["high_limited"] = main_info["high_limited"]
            main_df["low_limited"] = main_info["low_limited"]

            sub_df = load_contract_data(sub_contract, d, timescale_crud)
            sub_df["high_limited"] = sub_info["high_limited"]
            sub_df["low_limited"] = sub_info["low_limited"]

            # 加载前一交易日的数据
            pre_main_df = load_contract_data(main_contract, d, timescale_crud, previous=True)
            pre_sub_df = load_contract_data(sub_contract, d, timescale_crud, previous=True)

            # 拼接前一日尾部数据（通常为夜盘收尾）
            last_main_row = pre_main_df[pre_main_df["instrument_id"] == main_contract].tail(6).copy()
            main_df = pd.concat([last_main_row, main_df], ignore_index=True)

            last_sub_row = pre_sub_df[pre_sub_df["instrument_id"] == sub_contract].tail(6).copy()
            sub_df = pd.concat([last_sub_row, sub_df], ignore_index=True)

            systemLogger.info(
                f"数据加载成功: 日期: {d}, 主力行数={len(sub_df)}, 次主力行数={len(sub_df)}"
            )

            # 合约与 tick
            main_c = str(main_df.iloc[0]["instrument_id"])
            sub_c  = str(sub_df.iloc[0]["instrument_id"])
            tick   = get_tick_size(main_c, timescale_crud) or 0.1
            ticks.append(float(tick))

            # 过滤与对齐
            main_df = main_df[["instrument_id","trading_day","time","bid_price0","ask_price0","high_limited","low_limited"]]
            sub_df  = sub_df[["instrument_id","trading_day","time","bid_price0","ask_price0","high_limited","low_limited"]]

            main_df = remove_open_close_noise(main_df)
            sub_df  = remove_open_close_noise(sub_df)

            aligned = align_main_sub_minute(main_df, sub_df, float(tick))

            if aligned.empty or "spread" not in aligned.columns:
                systemLogger.warning(f"周样本对齐为空: 合约={main_c}{sub_c}, 交易日={d}")
                continue

            # 方向统一为 近月-远月
            _near, _far, sign = _is_main_near(main_c, sub_c)[1], _is_main_near(main_c, sub_c)[2], _is_main_near(main_c, sub_c)[3]
            spread_to_plot = aligned["spread"].astype(float) * int(sign)
            weekly_parts.append(spread_to_plot.dropna())

        weekly_spread = pd.concat(weekly_parts, ignore_index=True) if weekly_parts else pd.Series(dtype="float64")
        base_tick = float(min(ticks)) if ticks else 0.1

        # 统一分箱（仅用于绘图），对极端值做轻量 winsorize 确保直方图可读
        bins = None
        if not weekly_spread.empty:
            q1, q99 = weekly_spread.quantile(0.01), weekly_spread.quantile(0.99)
            rng_min = np.floor(q1 / base_tick) * base_tick
            rng_max = np.ceil(q99 / base_tick) * base_tick
            step = base_tick
            n_bins = int(np.round((rng_max - rng_min) / step)) + 1
            if n_bins > 400:
                factor = int(np.ceil(n_bins / 400.0))
                step = base_tick * factor
            bins = np.arange(rng_min, rng_max + step, step)

        weekly_range = (days[0], days[-1])
        return weekly_spread, weekly_range, bins, base_tick

    except Exception as e:
        systemLogger.exception(f"❌ 构建周样本失败: 交易日={tradingday}, 错误: {e}")
        return pd.Series(dtype="float64"), (tradingday, tradingday), None, 0.1
def _make_histograms(spread_series, tick_size, weekly_spread=None, window_days=None):
    """
    直方图（以 tick 为网格）：
    - bins 取“今天 + 近N日”的并集范围（保证近5日有、今天没有的桶也会出现，今天计数为0）
    - x 轴使用每个桶的“左边界真实值”（不取中心值），严格等于 tick 的倍数
    - 用整数化统计规避浮点累计误差
    返回 (hist_today, hist_week_or_None)
    """
    import numpy as np
    import pandas as pd

    tick = float(tick_size) if float(tick_size) > 0 else 0.1

    def to_grid_idx(vals):
        if vals is None:
            return np.array([], dtype=int)
        a = pd.Series(vals).dropna().astype(float).values
        if a.size == 0:
            return np.array([], dtype=int)
        # 把真实值映射到“tick 网格”的整数索引；rint=四舍五入，稳住可能的微小误差
        return np.rint(a / tick).astype(int)

    idx_today = to_grid_idx(spread_series)
    idx_week  = to_grid_idx(weekly_spread)

    if idx_today.size == 0 and idx_week.size == 0:
        return {"bins": [], "counts": []}, None

    # bins 边界的索引区间 = “今天 ∪ 近N日”的并集的最小/最大 + 1
    mins = []
    maxs = []
    if idx_today.size:
        mins.append(int(idx_today.min()))
        maxs.append(int(idx_today.max()))
    if idx_week.size:
        mins.append(int(idx_week.min()))
        maxs.append(int(idx_week.max()))
    lo = min(mins)
    hi = max(maxs)
    if lo == hi:
        lo -= 1
        hi += 1

    # 整数域的边界（注意：len(edges)=桶数+1）
    edges_idx = np.arange(lo, hi + 1 + 1, 1)

    # 统计：今天
    if idx_today.size:
        counts_today, _ = np.histogram(idx_today, bins=edges_idx)
    else:
        counts_today = np.zeros(len(edges_idx) - 1, dtype=int)

    # 统计：近N日
    hist_week = None
    if idx_week.size:
        counts_week, _ = np.histogram(idx_week, bins=edges_idx)
        hist_week = {
            # 左边界真实值（不取中心）
            "bins": [0.0 if abs(x) < 1e-12 else float(x)
                     for x in (edges_idx[:-1] * tick).astype(float)],
            "counts": [int(c) for c in counts_week.tolist()],
            "window_trading_days": int(window_days or 5)
        }

    # 今天的直方图（与近N日 bins 对齐）
    hist_today = {
        "bins": [0.0 if abs(x) < 1e-12 else float(x)
                 for x in (edges_idx[:-1] * tick).astype(float)],  # 左边界，不取中心
        "counts": [int(c) for c in counts_today.tolist()]
    }

    return hist_today, hist_week


# ===== 路由 =====
@app.route("/user/login", methods=["POST", "OPTIONS"])
def login():
    if request.method == "OPTIONS":
        return ("", 204)
    body = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""

    user = User.query.filter_by(username=username).first()
    if not user:
        return make_err(code=40101, message="用户名错误", http_status=401)
    if not check_password_hash(user.password_hash, password):
        return make_err(code=40101, message="密码错误", http_status=401)

    token = generate_token(username)
    return make_ok({"token": token})

@app.route("/user/info", methods=["GET", "OPTIONS"])
def info():
    if request.method == "OPTIONS":
        return ("", 204)

    token = request.headers.get("X-Token")
    if not token:
        auth = request.headers.get("Authorization", "")
        if auth.lower().startswith("bearer "):
            token = auth.split(" ", 1)[1].strip()
    if not token:
        token = request.args.get("token")
    if not token:
        return make_err(code=50008, message="Missing token", http_status=401)

    payload, err = verify_token(token)
    if err:
        msg, code = err
        return make_err(code=code, message=msg, http_status=401)

    user = User.query.filter_by(username=payload.get("sub")).first()
    if not user:
        return make_err(code=50008, message="User not found", http_status=401)

    data = {"name": user.name, "avatar": user.avatar, "roles": user.roles}
    return make_ok(data)

@app.route("/user/logout", methods=["POST", "OPTIONS"])
def logout():
    if request.method == "OPTIONS":
        return ("", 204)
    return make_ok(None)


# @app.route('/api/spread/today/dashboard', methods=['GET'])
@app.route('/spread/today/dashboard', methods=['GET'])
def api_spread_today_dashboard():

    start_time = time.time()

    # —— 微型剖析：标记每个阶段耗时 ——
    t0 = time.perf_counter()
    def _mark(tag):
        systemLogger.info(f"[PROF] {tag} +{(time.perf_counter()-t0)*1000:.1f}ms")
    _mark("enter")  # 进路由的第一行，应该基本“立刻”出现

    product = (request.args.get('product') or '').upper().strip()
    trading_day = (request.args.get('trading_day') or '').strip()  # 'YYYY-MM-DD'
    sessions = (request.args.get('sessions') or 'all').lower()
    group_size = int(request.args.get('group_size') or 10)

    near_ctp = (request.args.get('near_contract') or '').strip().upper()
    far_ctp  = (request.args.get('far_contract')  or '').strip().upper()
    contract_mode = (request.args.get('contract_mode') or '').strip().lower()

    weekly_window_trading_days = int(request.args.get('weekly_days') or 5)

    # 解析参数后
    _mark("parsed_params")

    # ====== 缓存：命中直接返回 ======
    key = _build_key(
        prefix="today:dashboard",
        params=dict(product=product, trading_day=trading_day, sessions=sessions,
                    group_size=group_size, near_ctp=near_ctp, far_ctp=far_ctp,
                    contract_mode=contract_mode, weekly_days=weekly_window_trading_days),
        include=["product","trading_day","sessions","group_size","near_ctp","far_ctp","contract_mode","weekly_days"]
    )
    cached = _cache_get_bytes(key)
    if cached:
        return app.response_class(cached, mimetype="application/json")


    is_manual = contract_mode == 'manual' or (near_ctp and far_ctp)

    if not trading_day:
        return make_err(code=40001, message='missing trading_day', http_status=400)
    # 自动模式必须带 product；手动模式允许不带
    if not is_manual and not product:
        return make_err(code=40001, message='missing product', http_status=400)

    # 统一成 date，避免 loader 里用字符串比较导致 miss
    # trading_day = pd.Timestamp(trading_day_str).date()

    # 手动模式如未传 product，尝试从合约前缀推断（如 CU2510 → cu）
    if is_manual and not product:
        m = re.match(r'([A-Za-z]+)\d+', near_ctp or '')
        if m:
            product = m.group(1).lower()
        # 如果是中金，大写
        if product in ["im", "ic", "if", "ih"]:
            product = product.upper()
    try:
        # # ====== 防止击穿：未命中则加锁，别人算时我短暂等 ======
        # with _Lock(key, ex=30) as got:
        #     _mark("lock.acquire")
        #     if not got:
        #         # 没拿到锁 -> 等待别人回填
        #         cached2 = _spin_wait_for_cache(key, wait_ms=120, tries=50)  # 最多 ~6s
        #         if cached2:
        #             return app.response_class(cached2, mimetype="application/json")
        # 现在再打你原来的第一行
        systemLogger.info(f"[TODAY] mode={'manual' if is_manual else 'auto'} product={product} day={trading_day} sessions={sessions}")
        _mark("before_load_main_sub")

        if is_manual:
            systemLogger.info(f"[TODAY] mode=manual near={near_ctp} far={far_ctp} day={trading_day} sessions={sessions}")
            # ✅ 传 date 类型
            main_df, sub_df, main_info, sub_info = load_pair_data(timescale_crud, trading_day, near_ctp, far_ctp)
        else:
            systemLogger.info(f"[TODAY] mode=auto product={product} day={trading_day} sessions={sessions}")
            main_df, sub_df, main_info, sub_info = load_main_sub_data(timescale_crud, trading_day, product)

        # 关键日志：立刻查看是否命中
        ms, ss = (main_df.shape if isinstance(main_df, pd.DataFrame) else ('NA','NA')), \
                 (sub_df.shape  if isinstance(sub_df,  pd.DataFrame) else ('NA','NA'))
        systemLogger.info(f"[TODAY] raw shapes: main={ms}, sub={ss}")

        if (main_df is None) or (sub_df is None) or main_df.empty or sub_df.empty:
            return make_ok({"summary": None, "histogram": None, "trend": None}, message='no data: raw empty')

        # 规范化合约名（有些表里是大写）
        main_contract = str(main_df.iloc[0]["instrument_id"])
        sub_contract  = str(sub_df.iloc[0]["instrument_id"])

        # 2) 清洗、时段过滤
        main_df = main_df[["instrument_id","trading_day","time","volume","bid_price0","ask_price0","high_limited","low_limited"]].copy()
        sub_df  = sub_df[ ["instrument_id","trading_day","time","volume","bid_price0","ask_price0","high_limited","low_limited"]].copy()

        # 5) 当日成交量&月均量
        trade_date = pd.to_datetime(trading_day).date()
        vol_main = int(main_df[main_df["trading_day"] == trade_date]["volume"].sum())
        vol_sub  = int(sub_df[sub_df["trading_day"] == trade_date]["volume"].sum())


        # 6) 展示口径：近月-远月；主力为远月则取反
        main_is_near, near_c, far_c, sign = _is_main_near(main_contract, sub_contract)

        # === NEW: 开盘窗口（每分钟）仅股指优先 09:32–10:00，否则默认 09:02–09:30 ===
        def _prod_prefix(ctp: str) -> str:
            m = re.match(r'([A-Za-z]+)\d*', ctp or '')
            return (m.group(1).upper() if m else '').upper()

        prod_prefix = _prod_prefix(near_c) or _prod_prefix(far_c) or (product or '').upper()
        is_index = prod_prefix in {"IF", "IH", "IC", "IM"}

        # 从“原始未按会话过滤”的数据里切出开盘窗口会更稳
        # 这里直接基于 main_df/sub_df（当前已做过 remove_open_close_noise + sessions 过滤）
        # 如果你更希望独立于 sessions，建议用原始 raw_main/raw_sub 再切；这里先按你现有变量来。
        def _slice_open_window(df: pd.DataFrame) -> pd.DataFrame:
            if df is None or df.empty:
                return df
            # 开盘窗口
            open_start = "09:32" if is_index else "09:02"
            open_end   = "10:00" if is_index else "09:30"
            t = pd.to_datetime(df["time"])
            mask = (t.dt.strftime("%H:%M") >= open_start) & (t.dt.strftime("%H:%M") <= open_end)
            return df.loc[mask].copy()

        # 你静态脚本里是 remove_close_noise；此处可选：先去掉收盘噪声再切窗口
        open_main_df = _slice_open_window(remove_close_noise(main_df))
        open_sub_df  = _slice_open_window(remove_close_noise(sub_df))

        def filter_by_sessions(df: pd.DataFrame) -> pd.DataFrame:
            if sessions == 'all':
                return df
            if not product:
                return df  # 无 product 无法套规则，直接返回
            from datetime import time as dtime
            rules = TRADING_SESSION_RULES.get(product, [])
            if not rules:
                return df  # 没规则也直接返回，避免全被过滤掉

            day_rules, night_rules = [], []
            for s in rules:
                st = datetime.strptime(s[0], "%H:%M").time()
                et = datetime.strptime(s[1], "%H:%M").time()
                if st >= dtime(20,0) or et <= dtime(3,0):
                    night_rules.append((st, et))
                else:
                    day_rules.append((st, et))

            keep = []
            for _, row in df.iterrows():
                t = pd.to_datetime(row["time"]).time()
                def in_any(ranges):
                    for st, et in ranges:
                        if st <= et:
                            if st <= t <= et: return True
                        else:
                            if t >= st or t <= et: return True
                    return False
                if sessions == 'day':   keep.append(in_any(day_rules))
                elif sessions == 'night': keep.append(in_any(night_rules))
                else: keep.append(True)
            return df.loc[keep].copy()

        main_df = remove_open_close_noise(filter_by_sessions(main_df))
        sub_df  = remove_open_close_noise(filter_by_sessions(sub_df))

        # 3) 对齐与价差
        tick_size = float(get_tick_size(main_contract, timescale_crud) or 0.1)
        aligned_df = align_main_sub_minute(main_df, sub_df, tick_size)
        aligned_df, limit_timestamps = filter_spread_with_limit(aligned_df)

        if aligned_df.empty:
            return make_ok({"summary": None, "histogram": None, "trend": None}, message='no data: aligned empty')

        spread = aligned_df['spread']
        spread_diff = spread.diff()
        spread_diff.index = aligned_df['time']

        # 4) 单腿 diff、波动率、相关性
        leg1_diff = compute_leg_return_series(main_df)
        leg2_diff = compute_leg_return_series(sub_df)

        corr = compute_spread_leg_correlation(spread_diff, leg1_diff, leg2_diff)


        # 你已有 get_avg_volume(instrument_id, end_date, days=30)
        from tradeAssistantSpread import get_avg_volume

        # start_time = time.time()
        # main_avg = int(get_avg_volume(timescale_crud, main_contract, trading_day, days=30) or 0)
        # # main_avg = 0
        # end_time = time.time()
        # systemLogger.info(f"·······主力获取平均成交量耗时: {end_time - start_time}")
        # start_time = time.time()
        # sub_avg  = int(get_avg_volume(timescale_crud, sub_contract, trading_day, days=30) or 0)
        # # sub_avg = 0
        # end_time = time.time()
        # systemLogger.info(f"·········此主力获取平均成交量耗时: {end_time - start_time}")

        t0 = time.time()
        avg_map = get_avg_volume_many_cached(timescale_crud, [main_contract, sub_contract], trading_day, days=30)
        main_avg = int(avg_map.get(main_contract, 0))
        sub_avg  = int(avg_map.get(sub_contract, 0))
        systemLogger.info(f"avg_volume(batch+cache) {time.time()-t0:.3f}s")


        # ====== 展示口径：直方图用近月-远月 ======
        spread_display = aligned_df['spread'].astype(float) * float(sign)

        # ====== 近 5 个交易日样本 + 统一分箱（仅用于直方图叠加） ======
        bins_edges = None
        counts_weekly = None


        # systemLogger.info(f"sub_info: {sub_info}")
        # systemLogger.info(f"main_info: {main_info}")


        # 你已有的静态函数，直接复用（如果你项目里函数名不同，按你的来）
        # 期望它返回：weekly_spread（一维序列：近 5 个交易日所有分钟的“近-远”价差），
        #             weekly_range（可选）、hist_bins（边界数组）、_（忽略）
        try:
            weekly_spread, weekly_range, hist_bins, _ = build_weekly_spread_samples(
                main_info=main_info,        # 如果你的函数需要 main/sub 的元数据，填你项目里的对象
                sub_info=sub_info,         # 没有就保持 None；下方会自动回退用“当天范围”出 bins
                tradingday=pd.to_datetime(trading_day).date(),
                timescale_crud=timescale_crud,
                window_trading_days=weekly_window_trading_days
            )
            # systemLogger.info(f"[TODAY] weekly samples: {weekly_spread}")
        except Exception as _e:
            systemLogger.warning(f"weekly samples failed: {repr(_e)}")
            weekly_spread, weekly_range, hist_bins = None, None, None

        # 1) 确定 bins（边界）：优先用 weekly 返回的；否则用“当天范围”兜底
        tick = float(tick_size) if float(tick_size) > 0 else 0.1
        if hist_bins is not None and isinstance(hist_bins, np.ndarray) and hist_bins.size > 0:
            bins_edges = hist_bins.astype(float)
        else:
            s = spread_display.dropna().astype(float)
            if s.size > 0:
                min_s = float(np.floor(s.min() / tick) * tick)
                max_s = float(np.ceil(s.max()  / tick) * tick)
                bins_edges = np.arange(min_s, max_s + tick, tick)
            else:
                bins_edges = np.arange(-5*tick, 5*tick + tick, tick)

        # 2) 统计 weekly 背景的频数（若 weekly_spread 为空则不给）
        if isinstance(weekly_spread, (pd.Series, np.ndarray, list)):
            arr_week = pd.Series(weekly_spread).dropna().astype(float).values
            if arr_week.size > 0:
                counts_weekly, _ = np.histogram(arr_week, bins=bins_edges)

        # 3) 统一“中心点”给前端（沿用你原来前端的 x 轴类别）
        bins_centers = ((bins_edges[:-1] + bins_edges[1:]) / 2.0).round(6).tolist()

        # 4) 当天的频数（用相同 bins_edges；注意用 spread_display）
        counts_today, _ = np.histogram(spread_display.dropna().astype(float).values, bins=bins_edges)

        # 5) 组织返回体
        histogram = {
            "bins": [float(v) for v in bins_centers],          # 中心点
            "counts": [int(c) for c in counts_today.tolist()]  # 当天
        }
        histogram_weekly = None
        if counts_weekly is not None:
            histogram_weekly = {
                "bins": [float(v) for v in bins_centers],             # 同一组中心点
                "counts": [int(c) for c in counts_weekly.tolist()],   # 近 5 日背景
                "window_trading_days": int(weekly_window_trading_days)
            }
        open_trend = None
        y_range_trend_open = None
        open_limit_timestamps = []

        if open_main_df is not None and not open_main_df.empty and \
           open_sub_df is not None and not open_sub_df.empty:
            aligned_open_df = align_main_sub_minute(open_main_df, open_sub_df, float(tick_size))
            if not aligned_open_df.empty:
                aligned_open_df, open_limit_timestamps = filter_spread_with_limit(aligned_open_df)

                spread_open = aligned_open_df['spread']
                spread_open_diff = spread_open.diff()
                spread_open_diff.index = aligned_open_df['time']
                open_exch_times = aligned_open_df['time']

                # 1) 横轴（时间）
                times_open = pd.to_datetime(open_exch_times)
                # 2) 对齐一分频 diff（首分钟 NaN→0 仅用于画图），展示口径：近月-远月
                s_open = pd.Series(spread_open_diff, index=pd.to_datetime(spread_open_diff.index))
                s_aligned = s_open.reindex(times_open)
                open_y_vals = (s_aligned.fillna(0).astype(float) * float(sign)).tolist()
                open_x_labels = pd.Series(times_open).dt.strftime('%H:%M').tolist()

                open_trend = {
                    "x": open_x_labels,
                    "y": open_y_vals,
                    "limit_ts": [pd.to_datetime(ts).strftime("%Y-%m-%d %H:%M:%S") for ts in (open_limit_timestamps or [])],
                    "window": {"start": "09:32" if is_index else "09:02",
                               "end":   "10:00" if is_index else "09:30"},
                    "is_index": bool(is_index)
                }

        spread_total_change_display = float((spread_diff.dropna().sum() or 0.0) * sign)


        # 8) 趋势：每 N 分钟聚合价差变化量（与你现有脚本一致）
        # 将 time 作为索引，按 group_size 分组求和
        df_trend = spread_diff.dropna().to_frame('diff')
        idx = pd.DatetimeIndex(df_trend.index)
        idx = idx.tz_localize('Asia/Shanghai') if idx.tz is None else idx.tz_convert('Asia/Shanghai')
        df_trend['bucket'] = idx.floor(f'{group_size}min')
        g = df_trend.groupby('bucket')['diff'].sum().reset_index()
        x = g['bucket'].dt.strftime('%H:%M').tolist()
        y = g['diff'].astype(float).round(6).tolist()
        # 展示口径：主力为远月则取反
        if sign == -1:
            y = [float(v) * -1.0 for v in y]

        # === NEW: 若存在开盘窗口数据，则给出与主趋势统一的 y 轴范围 ===
        y_range_trend_open = None
        if open_trend is not None:
            y2 = np.asarray(y, dtype=float)
            y3 = np.asarray(open_trend["y"], dtype=float)
            valid2 = y2[np.isfinite(y2)]
            valid3 = y3[np.isfinite(y3)]
            if valid2.size + valid3.size > 0:
                all_vals = np.concatenate([valid2, valid3])
                max_abs = np.max(np.abs(all_vals))
                max_abs = max(max_abs, 1e-6)  # 防全 0
                pad = max_abs * 0.08
                y_min, y_max = float(-max_abs - pad), float(max_abs + pad)
                y_range_trend_open = {"min": y_min, "max": y_max}


        # 9) 输出 summary
        summary = {
          "product": product.upper(),
          "trading_day": trading_day,
          "tick_size": tick_size,
          "main_contract": main_contract,
          "sub_contract": sub_contract,
          "near_contract": near_c,
          "far_contract": far_c,
          "main_limit_flag": bool(aligned_df["main_limited_flag"].any()),
          "sub_limit_flag": bool(aligned_df["sub_limited_flag"].any()),
          "vol_main": vol_main,
          "vol_sub": vol_sub,
          "main_avg_month_volume": main_avg,
          "sub_avg_month_volume": sub_avg,
          "leg1_total_change": float(leg1_diff.dropna().sum() or 0.0),
          "leg2_total_change": float(leg2_diff.dropna().sum() or 0.0),
          "spread_total_change": float(spread_diff.dropna().sum() or 0.0),
          "spread_total_change_display": float(spread_total_change_display),
          "leg1_volatility": float(compute_leg_volatility(leg1_diff) or 0.0),
          "leg2_volatility": float(compute_leg_volatility(leg2_diff) or 0.0),
          "spread_volatility": float(compute_spread_volatility(spread_diff) or 0.0),
          "corr_spread_leg1": float(corr["spread_vs_leg1"] or 0.0),
          "corr_spread_leg2": float(corr["spread_vs_leg2"] or 0.0),
          "corr_leg1_leg2": float(corr["leg1_vs_leg2"] or 0.0)
        }

        histogram, histogram_weekly = _make_histograms(
            spread_series=spread_display,
            tick_size=tick_size,
            weekly_spread=weekly_spread,                 # 如果上面没成功拿到 weekly，就会是 None
            window_days=weekly_window_trading_days
        )
        trend = {"x": x, "y": y}

        end_time = time.time()
        consume_time = end_time - start_time
        systemLogger.info(f"消耗时间：{consume_time}")
        resp = make_ok({
            "summary": summary,
            "histogram": histogram,
            "histogram_weekly": histogram_weekly,
            "trend": trend,
            "open_trend": open_trend,
            "y_range_trend_open": y_range_trend_open
        })
        try:
            ttl = _ttl_by_trading_day(trading_day, hot_ttl=30, cold_ttl=24*3600)
            _cache_set_bytes(key, resp.get_data(), ttl)
        except Exception:
            pass
        return resp

    except Exception as e:
        systemLogger.exception(f"spread today dashboard failed: product={product}, day={trading_day}, err={e}")
        return make_err(code=50000, message='server error', http_status=500)

def _parse_params():
    product = (request.args.get('product') or '').upper().strip()
    trading_day = (request.args.get('trading_day') or '').strip()  # 'YYYY-MM-DD'
    sessions = (request.args.get('sessions') or 'all').lower()
    group_size = int(request.args.get('group_size') or 5)
    near_ctp = (request.args.get('near_contract') or '').upper().strip()
    far_ctp  = (request.args.get('far_contract')  or '').upper().strip()
    contract_mode = (request.args.get('contract_mode') or '').lower().strip()
    weekly_days = int(request.args.get('weekly_days') or 5)

    is_manual = (contract_mode == 'manual') or (near_ctp and far_ctp)
    if is_manual and not product:
        m = re.match(r'([A-Za-z]+)\d+', near_ctp or '')
        if m:
            product = m.group(1).upper()
    return dict(product=product, trading_day=trading_day, sessions=sessions, group_size=group_size,
                near_ctp=near_ctp, far_ctp=far_ctp, contract_mode=contract_mode,
                is_manual=is_manual, weekly_days=weekly_days)

def _prod_prefix(ctp: str) -> str:
    m = re.match(r'([A-Za-z]+)\d*', ctp or '')
    return (m.group(1).upper() if m else '').upper()

def _filter_by_sessions(df: pd.DataFrame, product: str, sessions: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if sessions == 'all':
        return df
    if not product:
        return df
    from datetime import time as dtime
    rules = TRADING_SESSION_RULES.get(product, [])
    if not rules:
        return df
    day_rules, night_rules = [], []
    for s in rules:
        st = datetime.strptime(s[0], "%H:%M").time()
        et = datetime.strptime(s[1], "%H:%M").time()
        if st >= dtime(20,0) or et <= dtime(3,0):
            night_rules.append((st, et))
        else:
            day_rules.append((st, et))
    keep = []
    for _, row in df.iterrows():
        t = pd.to_datetime(row["time"]).time()
        def in_any(ranges):
            for st, et in ranges:
                if st <= et:
                    if st <= t <= et: return True
                else:
                    if t >= st or t <= et: return True
            return False
        if sessions == 'day':   keep.append(in_any(day_rules))
        elif sessions == 'night': keep.append(in_any(night_rules))
        else: keep.append(True)
    return df.loc[keep].copy()

def _slice_open_window(df: pd.DataFrame, is_index: bool) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    open_start = "09:32" if is_index else "09:02"
    open_end   = "10:00" if is_index else "09:30"
    t = pd.to_datetime(df["time"])
    mask = (t.dt.strftime("%H:%M") >= open_start) & (t.dt.strftime("%H:%M") <= open_end)
    return df.loc[mask].copy()
def _prepare_main_sub(timescale_crud, *, product, trading_day, sessions,
                      is_manual, near_ctp, far_ctp):
    """
    返回一个上下文字典 ctx，便于各子接口复用：
      ctx = {
        'product','trading_day','main_df','sub_df','aligned_df','spread_series','spread_diff',
        'tick_size','sign','near_c','far_c','main_contract','sub_contract',
        'vol_main','vol_sub','main_avg','sub_avg','leg1_diff','leg2_diff','corr',
        'limit_timestamps','is_index','open_main_df','open_sub_df','main_info','sub_info'
      }
    """
    # 1) 加载数据
    if is_manual:
        systemLogger.info(f"[TODAY/prepare] mode=manual near={near_ctp} far={far_ctp} day={trading_day} sessions={sessions}")
        main_df, sub_df, main_info, sub_info = load_pair_data(timescale_crud, trading_day, near_ctp, far_ctp)
    else:
        systemLogger.info(f"[TODAY/prepare] mode=auto product={product} day={trading_day} sessions={sessions}")
        main_df, sub_df, main_info, sub_info = load_main_sub_data(timescale_crud, trading_day, product)

    if (main_df is None) or (sub_df is None) or main_df.empty or sub_df.empty:
        return None  # 上层处理 no data

    # 2) 规范列 + 会话过滤 + 去开收盘噪声
    main_contract = str(main_df.iloc[0]["instrument_id"])
    sub_contract  = str(sub_df.iloc[0]["instrument_id"])

    cols = ["instrument_id","trading_day","time","volume","bid_price0","ask_price0","high_limited","low_limited"]
    main_df = main_df[cols].copy()
    sub_df  = sub_df[cols].copy()

    trade_date = pd.to_datetime(trading_day).date()
    vol_main = int(main_df[main_df["trading_day"] == trade_date]["volume"].sum())
    vol_sub  = int(sub_df[sub_df["trading_day"] == trade_date]["volume"].sum())

    main_df = remove_open_close_noise(_filter_by_sessions(main_df, product, sessions))
    sub_df  = remove_open_close_noise(_filter_by_sessions(sub_df,  product, sessions))

    # 3) 对齐 + 价差
    tick_size = float(get_tick_size(main_contract, timescale_crud) or 0.1)
    aligned_df = align_main_sub_minute(main_df, sub_df, tick_size)
    aligned_df, limit_timestamps = filter_spread_with_limit(aligned_df)
    if aligned_df.empty:
        return None

    spread = aligned_df['spread']
    spread_diff = spread.diff()
    spread_diff.index = aligned_df['time']

    # 4) 单腿 diff + 相关
    leg1_diff = compute_leg_return_series(main_df)
    leg2_diff = compute_leg_return_series(sub_df)
    corr = compute_spread_leg_correlation(spread_diff, leg1_diff, leg2_diff)

    # 5) 月均量
    from tradeAssistantSpread import get_avg_volume
    main_avg = int(get_avg_volume(timescale_crud, main_contract, trading_day, days=30) or 0)
    sub_avg  = int(get_avg_volume(timescale_crud, sub_contract, trading_day, days=30) or 0)

    # 6) 展示口径（近-远）
    main_is_near, near_c, far_c, sign = _is_main_near(main_contract, sub_contract)
    spread_display = aligned_df['spread'].astype(float) * float(sign)

    # 7) 开盘窗口（供 open_trend 用）
    prod_prefix = _prod_prefix(near_c) or _prod_prefix(far_c) or (product or '').upper()
    is_index = prod_prefix in {"IF", "IH", "IC", "IM"}
    open_main_df = _slice_open_window(remove_close_noise(main_df), is_index)
    open_sub_df  = _slice_open_window(remove_close_noise(sub_df),  is_index)

    return dict(product=product, trading_day=trading_day,
                main_df=main_df, sub_df=sub_df, aligned_df=aligned_df,
                spread_series=spread_display, spread_diff=spread_diff,
                tick_size=tick_size, sign=sign, near_c=near_c, far_c=far_c,
                main_contract=main_contract, sub_contract=sub_contract,
                vol_main=vol_main, vol_sub=vol_sub, main_avg=main_avg, sub_avg=sub_avg,
                leg1_diff=leg1_diff, leg2_diff=leg2_diff, corr=corr,
                limit_timestamps=limit_timestamps, is_index=is_index,
                open_main_df=open_main_df, open_sub_df=open_sub_df,
                main_info=main_info, sub_info=sub_info)
@app.get('/spread/today/summary')
def api_summary():
    p = _parse_params()
    try:
        ctx = _prepare_main_sub(timescale_crud, **p)
        if ctx is None:
            return make_ok({"summary": None}, message='no data')
        spread_total_change_display = float((ctx['spread_diff'].dropna().sum() or 0.0) * ctx['sign'])
        summary = {
          "product": ctx['product'],
          "trading_day": p['trading_day'],
          "tick_size": ctx['tick_size'],
          "main_contract": ctx['main_contract'],
          "sub_contract": ctx['sub_contract'],
          "near_contract": ctx['near_c'],
          "far_contract": ctx['far_c'],
          "main_limit_flag": bool(ctx['aligned_df']["main_limited_flag"].any()),
          "sub_limit_flag": bool(ctx['aligned_df']["sub_limited_flag"].any()),
          "vol_main": ctx['vol_main'],
          "vol_sub": ctx['vol_sub'],
          "main_avg_month_volume": ctx['main_avg'],
          "sub_avg_month_volume": ctx['sub_avg'],
          "leg1_total_change": float(ctx['leg1_diff'].dropna().sum() or 0.0),
          "leg2_total_change": float(ctx['leg2_diff'].dropna().sum() or 0.0),
          "spread_total_change": float(ctx['spread_diff'].dropna().sum() or 0.0),
          "spread_total_change_display": spread_total_change_display,
          "leg1_volatility": float(compute_leg_volatility(ctx['leg1_diff']) or 0.0),
          "leg2_volatility": float(compute_leg_volatility(ctx['leg2_diff']) or 0.0),
          "spread_volatility": float(compute_spread_volatility(ctx['spread_diff']) or 0.0),
          "corr_spread_leg1": float(ctx['corr']["spread_vs_leg1"] or 0.0),
          "corr_spread_leg2": float(ctx['corr']["spread_vs_leg2"] or 0.0),
          "corr_leg1_leg2": float(ctx['corr']["leg1_vs_leg2"] or 0.0)
        }
        return make_ok({"summary": summary})
    except Exception as e:
        systemLogger.exception(f"summary failed: {e}")
        return make_ok({"summary": None}, message='error')

def _make_histograms(spread_series, tick_size, weekly_spread=None, window_days=5):
    # 与你大接口一致：优先 weekly 设边界；否则按当天范围
    tick = float(tick_size) if float(tick_size) > 0 else 0.1
    s_today = pd.Series(spread_series).dropna().astype(float)
    bins_edges = None
    if weekly_spread is not None:
        s_week = pd.Series(weekly_spread).dropna().astype(float)
        if s_week.size > 0:
            eps = 1e-9 * abs(tick)
            min_s = float(np.floor(s_week.min() / tick) * tick)
            max_s = float(np.ceil( s_week.max() / tick) * tick)
            bins_edges = np.arange(min_s, max_s + tick + eps, tick)
    if bins_edges is None:
        if s_today.size > 0:
            min_s = float(np.floor(s_today.min() / tick) * tick)
            max_s = float(np.ceil( s_today.max() / tick) * tick)
            bins_edges = np.arange(min_s, max_s + tick, tick)
        else:
            bins_edges = np.arange(-5*tick, 5*tick + tick, tick)

    centers = ((bins_edges[:-1] + bins_edges[1:]) / 2.0).round(6)
    counts_today, _ = np.histogram(s_today.values, bins=bins_edges)
    histogram = {
        "bins": [float(v) for v in centers.tolist()],
        "counts": [int(c) for c in counts_today.tolist()]
    }
    histogram_weekly = None
    if weekly_spread is not None:
        s_week = pd.Series(weekly_spread).dropna().astype(float)
        if s_week.size > 0:
            counts_week, _ = np.histogram(s_week.values, bins=bins_edges)
            histogram_weekly = {
                "bins": [float(v) for v in centers.tolist()],
                "counts": [int(c) for c in counts_week.tolist()],
                "window_trading_days": int(window_days)
            }
    return histogram, histogram_weekly

@app.get('/spread/today/histogram')
def api_histogram():
    p = _parse_params()
    try:
        ctx = _prepare_main_sub(timescale_crud, **p)
        if ctx is None:
            return make_ok({"histogram": None}, message='no data')
        hist, _ = _make_histograms(spread_series=ctx['spread_series'],
                                   tick_size=ctx['tick_size'],
                                   weekly_spread=None,
                                   window_days=p['weekly_days'])
        return make_ok({"histogram": hist})
    except Exception as e:
        systemLogger.exception(f"histogram failed: {e}")
        return make_ok({"histogram": None}, message='error')




@app.get('/spread/today/trend')
def api_trend():
    p = _parse_params()
    try:
        ctx = _prepare_main_sub(timescale_crud, **p)
        if ctx is None:
            return make_ok({"trend": None}, message='no data')
        df_trend = ctx['spread_diff'].dropna().to_frame('diff')
        idx = pd.DatetimeIndex(df_trend.index)
        idx = idx.tz_localize('Asia/Shanghai') if idx.tz is None else idx.tz_convert('Asia/Shanghai')
        df_trend['bucket'] = idx.floor(f"{p['group_size']}min")
        g = df_trend.groupby('bucket')['diff'].sum().reset_index()
        x = g['bucket'].dt.strftime('%H:%M').tolist()
        y = g['diff'].astype(float).round(6).tolist()
        if ctx['sign'] == -1:
            y = [float(v) * -1.0 for v in y]
        return make_ok({"trend": {"x": x, "y": y}})
    except Exception as e:
        systemLogger.exception(f"trend failed: {e}")
        return make_ok({"trend": None}, message='error')



@app.get('/spread/today/open_trend')
def api_open_trend():
    p = _parse_params()
    try:
        ctx = _prepare_main_sub(timescale_crud, **p)
        if ctx is None:
            return make_ok({"open_trend": None, "y_range_trend_open": None}, message='no data')

        open_trend = None
        y_range_trend_open = None

        if ctx['open_main_df'] is not None and not ctx['open_main_df'].empty \
           and ctx['open_sub_df'] is not None and not ctx['open_sub_df'].empty:
            aligned_open_df = align_main_sub_minute(ctx['open_main_df'], ctx['open_sub_df'], float(ctx['tick_size']))
            if not aligned_open_df.empty:
                aligned_open_df, open_limit_ts = filter_spread_with_limit(aligned_open_df)
                spread_open = aligned_open_df['spread']
                spread_open_diff = spread_open.diff()
                spread_open_diff.index = aligned_open_df['time']
                times_open = pd.to_datetime(aligned_open_df['time'])

                s_open = pd.Series(spread_open_diff, index=pd.to_datetime(spread_open_diff.index))
                s_aligned = s_open.reindex(times_open)
                open_y_vals = (s_aligned.fillna(0).astype(float) * float(ctx['sign'])).tolist()
                open_x_labels = pd.Series(times_open).dt.strftime('%H:%M').tolist()

                open_trend = {
                    "x": open_x_labels,
                    "y": open_y_vals,
                    "limit_ts": [pd.to_datetime(ts).strftime("%Y-%m-%d %H:%M:%S") for ts in (open_limit_ts or [])],
                    "window": {"start": "09:32" if ctx['is_index'] else "09:02",
                               "end":   "10:00" if ctx['is_index'] else "09:30"},
                    "is_index": bool(ctx['is_index'])
                }

                # 与主趋势统一 y 轴范围（这里简单用“开盘自身”做范围，前端拿到后可覆盖主趋势）
                vals = np.asarray(open_y_vals, dtype=float)
                vals = vals[np.isfinite(vals)]
                if vals.size > 0:
                    max_abs = max(np.max(np.abs(vals)), 1e-6)
                    pad = max_abs * 0.08
                    y_range_trend_open = {"min": float(-max_abs - pad), "max": float(max_abs + pad)}

        return make_ok({"open_trend": open_trend, "y_range_trend_open": y_range_trend_open})
    except Exception as e:
        systemLogger.exception(f"open_trend failed: {e}")
        return make_ok({"open_trend": None, "y_range_trend_open": None}, message='error')

def _get_trading_days_cached(start_d: date, end_d: date) -> List[date]:
    key = _build_key("his:days", {"start": str(start_d), "end": str(end_d)}, ["start","end"])
    b = _cache_get_bytes(key)
    if b:
        return [pd.to_datetime(x).date() for x in _jloads(b)]
    with ts_crud.session_scope() as session:
        days = _iter_trading_days(session, start_d, end_d)
    ttl = 24*3600
    _cache_set_bytes(key, _jdumps([d.strftime("%Y-%m-%d") for d in days]), ttl)
    return days

def _pick_main_sub_cached(session, product: str, d: date):
    key = _build_key("his:pick", {"product": product, "day": d.strftime("%Y-%m-%d")}, ["product","day"])
    b = _cache_get_bytes(key)
    if b:
        v = _jloads(b)   # {"main":..., "sub":..., "reason":...} 或 {"none":1}
        if "none" in v: return None, None, "no_data"
        return v["main"], v["sub"], v["reason"]

    main_d, sub_d, reason = _pick_main_sub_by_volume_first(session, product, d)
    ttl = _ttl_by_trading_day(d.strftime("%Y-%m-%d"), hot_ttl=60, cold_ttl=12*3600)
    if not main_d or not sub_d:
        _cache_set_bytes(key, _jdumps({"none": 1}), ttl)
        return None, None, "no_data"
    _cache_set_bytes(key, _jdumps({"main": main_d, "sub": sub_d, "reason": reason}), ttl)
    return main_d, sub_d, reason

def _load_minute_df_cached(session, inst: str, d: date) -> pd.DataFrame:
    key = _build_key("his:bars", {"inst": inst, "day": d.strftime("%Y-%m-%d")}, ["inst","day"])
    b = _cache_get_bytes(key)
    if b:
        return _df_unpack(b)
    df = _load_minute_df(session, inst, d)
    ttl = _ttl_by_trading_day(d.strftime("%Y-%m-%d"), hot_ttl=120, cold_ttl=7*24*3600)
    # 空结果也负缓存，避免反复打 DB
    payload = _df_pack(df) if not df.empty else b""
    _cache_set_bytes(key, payload, ttl)
    return df

def _calc_day_payload_cached(session, *, product, sessions, d: date,
                             auto_contract: bool, near_ctp: str, far_ctp: str, debug: bool):
    # Key 里区分自动/手动；手动要带 near/far
    key_params = {
        "product": product, "day": d.strftime("%Y-%m-%d"), "sessions": sessions,
        "mode": "auto" if auto_contract else "manual",
        "near": near_ctp if not auto_contract else "",
        "far":  far_ctp  if not auto_contract else ""
    }
    key = _build_key("his:day", key_params, ["product","day","sessions","mode","near","far"])
    b = _cache_get_bytes(key)
    if b:
        return _jloads(b)  # 直接返回 dict：可能是 {"skip_reason": "..."} 或 {"date":..., "near":..., "far":..., "metrics": {...}}

    # 选合约
    if auto_contract:
        main_d, sub_d, pick_reason = _pick_main_sub_cached(session, product, d)
        if not main_d or not sub_d:
            out = {"skip_reason": "no_data"}
            _cache_set_bytes(key, _jdumps(out), _ttl_by_trading_day(d.strftime("%Y-%m-%d"), hot_ttl=60, cold_ttl=12*3600))
            return out
        m_ym = _parse_contract_yyyymm(main_d) or 0
        s_ym = _parse_contract_yyyymm(sub_d) or 0
        near_d, far_d = (main_d, sub_d) if (m_ym and s_ym and m_ym <= s_ym) else (sub_d, main_d)
    else:
        near_d, far_d = near_ctp, far_ctp

    # 前一交易日
    prev_d = _prev_trading_day(session, d)

    # 拉数据（用 cached 版本）
    df_near_today = _load_minute_df_cached(session, near_d, d)
    df_near_prev  = _load_minute_df_cached(session, near_d, prev_d) if prev_d else pd.DataFrame()
    df_far_today  = _load_minute_df_cached(session, far_d,  d)
    df_far_prev   = _load_minute_df_cached(session, far_d,  prev_d) if prev_d else pd.DataFrame()

    if df_near_today.empty and df_near_prev.empty:
        out = {"skip_reason": "no_near_bar"}
        _cache_set_bytes(key, _jdumps(out), _ttl_by_trading_day(d.strftime("%Y-%m-%d"), hot_ttl=60, cold_ttl=6*3600))
        return out
    if df_far_today.empty and df_far_prev.empty:
        out = {"skip_reason": "no_far_bar"}
        _cache_set_bytes(key, _jdumps(out), _ttl_by_trading_day(d.strftime("%Y-%m-%d"), hot_ttl=60, cold_ttl=6*3600))
        return out

    # 拼接+会话过滤
    df_near_full = _concat_prev_and_today(df_near_prev, df_near_today, product, sessions)
    df_far_full  = _concat_prev_and_today(df_far_prev,  df_far_today,  product, sessions)
    if df_near_full.empty or df_far_full.empty:
        out = {"skip_reason": "filtered_empty"}
        _cache_set_bytes(key, _jdumps(out), _ttl_by_trading_day(d.strftime("%Y-%m-%d"), hot_ttl=60, cold_ttl=6*3600))
        return out

    m = _day_metrics(df_near_full, df_far_full)
    if not m:
        out = {"skip_reason": "no_metrics"}
        _cache_set_bytes(key, _jdumps(out), _ttl_by_trading_day(d.strftime("%Y-%m-%d"), hot_ttl=60, cold_ttl=6*3600))
        return out

    out = {
        "date": d.strftime("%Y-%m-%d"),
        "near": near_d,
        "far":  far_d,
        "metrics": m
    }
    # 历史长 TTL；如果是“今天”，短 TTL
    _cache_set_bytes(key, _jdumps(out), _ttl_by_trading_day(d.strftime("%Y-%m-%d"), hot_ttl=60, cold_ttl=24*3600))
    return out



@app.route('/spread/history/dashboard', methods=['GET'])
def api_spread_history_dashboard():
    try:
        product   = (request.args.get('product') or 'cu').strip().lower()
        sessions  = (request.args.get('sessions') or 'all').strip().lower()
        near_ctp  = (request.args.get('near_contract') or '').strip()
        far_ctp   = (request.args.get('far_contract')  or '').strip()
        auto_contract = not (near_ctp and far_ctp)

        start_str = request.args.get('start')
        end_str   = request.args.get('end')
        debug     = (request.args.get('debug') == '1')

        if not start_str or not end_str:
            return make_err(code=40001, message='start/end 参数必传', http_status=400)

        start = pd.Timestamp(start_str).date()
        end   = pd.Timestamp(end_str).date()

        # ==== 顶层缓存 Key ====
        key = _build_key("his:dashboard", dict(
            product=product, sessions=sessions, start=start_str, end=end_str,
            mode=("auto" if auto_contract else "manual"), near=near_ctp, far=far_ctp, debug=str(int(debug))
        ), ["product","sessions","start","end","mode","near","far","debug"])

        # 命中直接返回
        cached = _cache_get_bytes(key)
        if cached:
            return app.response_class(cached, mimetype="application/json")

        # 反击穿锁
        with _Lock(key, ex=20) as got:
            # if not got:
            #     cached2 = _spin_wait_for_cache(key, wait_ms=120, tries=50)
            #     if cached2:
            #         return app.response_class(cached2, mimetype="application/json")

            # ===== 正常计算流程（带子缓存）=====
            day_list = _get_trading_days_cached(start, end)
            reasons = defaultdict(int)

            x = []
            contracts = []
            metrics = dict(
                vol_near=[], vol_far=[],
                dP_near=[], dP_far=[],
                volat_near=[], volat_far=[],
                spread_delta=[], spread_volat=[],
                corr_spread_near=[], corr_spread_far=[]
            )

            with ts_crud.session_scope() as session:
                for d in day_list:
                    res = _calc_day_payload_cached(session,
                        product=product, sessions=sessions, d=d,
                        auto_contract=auto_contract, near_ctp=near_ctp, far_ctp=far_ctp, debug=debug
                    )
                    if "skip_reason" in res:
                        reasons[res["skip_reason"]] += 1
                        continue

                    x.append(res["date"])
                    contracts.append({"date": res["date"], "near": res["near"], "far": res["far"]})
                    m = res["metrics"]
                    for k in metrics.keys():
                        metrics[k].append(m.get(k))

            days_ok = len(x)
            summary = dict(
                days=days_ok,
                skipped=len(day_list) - days_ok,
                avg_vol_near=(int(np.mean(metrics['vol_near'])) if days_ok else None),
                avg_vol_far=(int(np.mean(metrics['vol_far'])) if days_ok else None),
                avg_volat_spread=(float(np.mean(metrics['spread_volat'])) if days_ok else None)
            )

            payload = {
                'x': x,
                'contracts': contracts,
                'metrics': metrics,
                'summary': summary,
                'params': {
                    'product': product.upper(),
                    'start': start.strftime('%Y-%m-%d'),
                    'end': end.strftime('%Y-%m-%d'),
                    'sessions': sessions,
                    'auto_contract': auto_contract,
                    'near_contract': near_ctp if not auto_contract else '',
                    'far_contract':  far_ctp  if not auto_contract else ''
                }
            }
            if debug:
                payload['debug'] = {'reason_counts': dict(reasons)}

            resp = make_ok(payload)
            # TTL：如果区间包含“今天”，给短 TTL；纯历史给长 TTL
            contains_today = any(_is_today_sh(d.strftime("%Y-%m-%d")) for d in day_list) if day_list else False
            ttl = 60 if contains_today else 6*3600
            _cache_set_bytes(key, resp.get_data(), ttl)
            return resp

    except Exception as e:
        systemLogger.exception(f"/spread/history/dashboard error: {e}")
        return make_err(code=50099, message=f"history dashboard error: {e}", http_status=500)


# 结算
@app.route("/pnl", methods=["GET"])
def pnl():
    # 从 token 解析用户名 & 角色
    token = request.headers.get("X-Token") or ...
    payload, err = verify_token(token)
    if err: return make_err(code=40101, message="未登录", http_status=401)

    user = User.query.filter_by(username=payload.get("sub")).first()
    roles = set(user.roles)

    if "admin" in roles:
        # 管理员：汇总全量
        data = {
            "totalPnL": 123456.78,
            "teamPnL":  [
              {"trader":"Alice","pnl": 1234.5},
              {"trader":"Bob",  "pnl": -321.0}
            ]
        }
    elif "trader" in roles:
        # Trader：只给本人或组内
        # 假设能查到其 group_id，再按 group_id 聚合
        data = {
            "myPnL":  456.78,
            "groupPnL": [
              {"member":"Me","pnl":456.78},
              {"member":"Teammate","pnl":-12.3}
            ]
        }
    else:
        return make_err(code=40300, message="无权限", http_status=403)

    return make_ok(data)


@app.route('/spread/contracts', methods=['GET'])
def api_spread_contracts():
    try:
        product = (request.args.get('product') or '').strip()
        on_str  = (request.args.get('on') or '').strip()
        if not product:
            return make_err(code=40001, message='product 必传', http_status=400)

        on_date = pd.Timestamp(on_str).date() if on_str else date.today()
        systemLogger.info(f"[CONTRACTS] in: product={product}, on={on_date}")

        with ts_crud.session_scope() as session:
            rows = (session.query(
                        ChinaFuturesBaseInfo.instrument_id,
                        ChinaFuturesBaseInfo.start_trade_date,
                        ChinaFuturesBaseInfo.last_trade_date,
                        ChinaFuturesBaseInfo.exchange
                    )
                    .filter(ChinaFuturesBaseInfo.product.ilike(product))
                    .all())

            systemLogger.info(f"[CONTRACTS] raw_count={len(rows)}")

            # 仅保留 on_date 在交易期内的合约；内部用 yyyymm 排序，但**不对外返回**
            opts = []
            for inst, start_d, last_d, _exch in rows:
                cond_start = (start_d is None) or (start_d <= on_date)
                cond_last  = (last_d  is None) or (on_date <= last_d)
                if not (cond_start and cond_last):
                    continue
                yyyymm = _parse_contract_yyyymm(inst) or 0
                opts.append({'inst': inst, 'yyyymm': yyyymm})

            # 按交割月+代码排序
            opts.sort(key=lambda x: (x['yyyymm'], x['inst']))

            # 生成默认近月/远月（只输出 instrument_id）
            default_near = opts[0]['inst'] if len(opts) >= 1 else ''
            default_far  = ''
            if len(opts) >= 2:
                default_far = opts[1]['inst']
                if default_far == default_near:
                    for o in opts[2:]:
                        if o['inst'] != default_near:
                            default_far = o['inst']
                            break

            # 对**外部**，只返回 instrument_id 列表
            options = [o['inst'] for o in opts]

            systemLogger.info(f"[CONTRACTS] out: default_near={default_near}, default_far={default_far}, options={len(options)}")
            return make_ok({
                'product': product,
                'on': on_date.strftime('%Y-%m-%d'),
                'options': options,              # 仅字符串数组
                'default_near': default_near,    # 字符串
                'default_far':  default_far      # 字符串
            })
    except Exception as e:
        systemLogger.exception(f"/spread/contracts error: {e}")
        return make_err(code=50099, message=f"contracts error: {e}", http_status=500)

@app.errorhandler(404)
def not_found(e):
    return make_err(code=40400, message="Not Found", http_status=404)

@app.errorhandler(405)
def method_not_allowed(e):
    return make_err(code=40500, message="Method Not Allowed", http_status=405)

if __name__ == "__main__":
    with app.app_context():
        init_db_and_seed()
    app.run(host="0.0.0.0", port=8080, debug=True)
