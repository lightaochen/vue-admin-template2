# app/services/spread_service.py
from __future__ import annotations
from datetime import datetime, timedelta, date
import re
import time
import numpy as np
import pandas as pd
import pytz
from typing import Any, Dict, Optional, Tuple, List
from flask import current_app

# 扩展/单例
from app.extensions import ts_crud, timescale_crud
from app.utils.response import make_ok, make_err
from app.utils.cache import (
    build_key, cache_get_bytes, cache_set_bytes,
    ttl_by_trading_day, RedisLock
)

# 你现有的 logger
from common.logger import systemLogger

# 规则仍沿用你原文件（也可迁入 app/config.py）
from config.settings import TRADING_SESSION_RULES, TRADING_Open_SESSION_RULES

# SQLAlchemy / 模型
from sqlalchemy import func, desc
from models.models import (
    ChinaTradingDay,
    VChinaFuturesDaybar,
    ChinaFuturesL1TABar,
    ChinaFuturesBaseInfo
)

# 你的算法工具（保持不动）
from tradeAssistantSpread import (
    load_main_sub_data, get_tick_size, remove_open_close_noise,
    align_main_sub_minute, filter_spread_with_limit,
    compute_leg_return_series, compute_leg_volatility,
    compute_spread_volatility, compute_spread_leg_correlation,
    _is_main_near, load_pair_data
)

# --------------------------------------------------------------------------------------
# 参数解析（供路由使用）
# --------------------------------------------------------------------------------------
def _parse_params(args):
    product = (args.get('product') or '').upper().strip()
    trading_day = (args.get('trading_day') or '').strip()  # 'YYYY-MM-DD'
    sessions = (args.get('sessions') or 'all').lower()
    group_size = int(args.get('group_size') or 10)
    near_ctp = (args.get('near_contract') or '').strip().upper()
    far_ctp  = (args.get('far_contract')  or '').strip().upper()
    contract_mode = (args.get('contract_mode') or '').strip().lower()
    weekly_window_trading_days = int(args.get('weekly_days') or 5)
    is_manual = (contract_mode == 'manual') or (near_ctp and far_ctp)

    # 手动模式未传 product，则由合约前缀推断
    if is_manual and not product:
        m = re.match(r'([A-Za-z]+)\d+', near_ctp or '')
        if m:
            product = m.group(1).upper()
        if product in ["IM", "IC", "IF", "IH"]:
            product = product.upper()

    return dict(
        product=product,
        trading_day=trading_day,
        sessions=sessions,
        group_size=group_size,
        near_ctp=near_ctp,
        far_ctp=far_ctp,
        contract_mode=contract_mode,
        is_manual=is_manual,
        weekly_days=weekly_window_trading_days
    )

# --------------------------------------------------------------------------------------
# 通用工具
# --------------------------------------------------------------------------------------
def _prod_prefix(ctp: str) -> str:
    m = re.match(r'([A-Za-z]+)\d*', ctp or '')
    return (m.group(1).upper() if m else '').upper()

def _is_today_sh(day_str: str) -> bool:
    if not day_str: return False
    try:
        tz = pytz.timezone("Asia/Shanghai")
        return pd.to_datetime(day_str).date() == datetime.now(tz).date()
    except Exception:
        return False

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
        df["__product__"] = df["instrument_id"].apply(lambda x: re.match(r'([A-Za-z]+)\d*', str(x)).group(1).lower() if re.match(r'([A-Za-z]+)\d*', str(x)) else "")

        def is_in_open_window(row):
            product = row["__product__"]
            t = row["__time_only__"]
            if pd.isnull(t):
                return False
            sessions = TRADING_Open_SESSION_RULES.get(product, None)
            if not sessions:
                return True
            for s in sessions:
                start = datetime.strptime(s[0], "%H:%M").time()
                end   = datetime.strptime(s[1], "%H:%M").time()
                if start <= end:
                    if start <= t <= end:
                        return True
                else:
                    if t >= start or t <= end:
                        return True
            return False

        df = df[df.apply(is_in_open_window, axis=1)].copy()
        return df.drop(columns=["__time_only__", "__product__"])
    except Exception as e:
        systemLogger.exception(f"❌ 开盘窗口过滤失败: 错误={e}")
        return df

# --------------------------------------------------------------------------------------
# 数据读取与缓存派生
# --------------------------------------------------------------------------------------
def get_avg_volume_many_cached(timescale_crud, instruments: List[str], end_date: str, days: int = 30) -> Dict[str, int]:
    instruments = [i for i in (instruments or []) if i]
    if not instruments or not end_date:
        return {i:0 for i in instruments}

    result, miss = {}, []
    ttl = ttl_by_trading_day(end_date, hot_ttl=300, cold_ttl=24*3600)
    for inst in instruments:
        k = f"ta:v1:avgvol:{inst.upper()}:{end_date}:{int(days)}"
        b = cache_get_bytes(k)
        if b:
            try:
                v = int(pd.read_json(b, typ='series') if False else int(b.decode("utf-8")))
            except Exception:
                # 兼容 orjson/bytes：直接强转
                try:
                    v = int(b)
                except Exception:
                    v = None
            if v is not None:
                result[inst] = v
                continue
        miss.append(inst)

    if not miss:
        return result

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
        k = f"ta:v1:avgvol:{inst.upper()}:{end_date}:{int(days)}"
        try:
            cache_set_bytes(k, str(avg_map[inst]).encode("utf-8"), ttl)
        except Exception:
            pass

    result.update(avg_map)
    return result

def _prev_trading_day(session, d: date) -> Optional[date]:
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

def load_contract_data(instrument_id: str, trading_day: str, timescale_crud, previous: bool = False) -> pd.DataFrame:
    try:
        sh_tz = pytz.timezone("Asia/Shanghai")
        target_day = datetime.strptime(trading_day, "%Y-%m-%d").date()

        with timescale_crud.session_scope() as session:
            if previous:
                pre_day_obj = session.query(ChinaTradingDay.pre_trading_day)\
                    .filter(ChinaTradingDay.trading_day == target_day).first()
                if not pre_day_obj:
                    return pd.DataFrame()
                target_day = pre_day_obj.pre_trading_day

            start_dt = datetime.combine(target_day - timedelta(days=14), datetime.min.time()).astimezone(pytz.utc)
            end_dt   = datetime.combine(target_day + timedelta(days=14), datetime.min.time()).astimezone(pytz.utc)

            rows = session.query(ChinaFuturesL1TABar)\
                .filter(ChinaFuturesL1TABar.time.between(start_dt, end_dt))\
                .filter(ChinaFuturesL1TABar.trading_day == target_day)\
                .filter(ChinaFuturesL1TABar.instrument_id == instrument_id)\
                .order_by(ChinaFuturesL1TABar.time)\
                .all()

            if not rows:
                return pd.DataFrame()

            df = pd.DataFrame([{**{col.name: getattr(row, col.name) for col in row.__table__.columns}} for row in rows])
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time']).dt.tz_convert(sh_tz)
            return df

    except Exception as e:
        systemLogger.exception(f"❌ 数据库查询失败: 合约={instrument_id}, 日期={trading_day}, previous={previous}, 错误: {e}")
        return pd.DataFrame()

def get_recent_trading_days(tradingday: str, n: int, timescale_crud) -> List[str]:
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

def _parse_contract_yyyymm(instr: str) -> Optional[int]:
    if not instr:
        return None
    m = re.search(r'(\d{4,5})$', instr)
    if not m:
        return None
    yy = m.group(1)
    try:
        y = 2000 + int(yy[:2])
        mth = int(yy[2:])
        return y * 100 + mth
    except:
        return None

def _concat_prev_and_today(df_prev: pd.DataFrame, df_today: pd.DataFrame, product: str, sessions: str) -> pd.DataFrame:
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

# --------------------------------------------------------------------------------------
# 近一周样本 & 直方图
# --------------------------------------------------------------------------------------
def build_weekly_spread_samples(
    main_info: dict,
    sub_info: dict,
    tradingday: str,
    timescale_crud,
    window_trading_days: int = 5
) -> Tuple[pd.Series, Tuple[str, str], Optional[np.ndarray], float]:
    try:
        days = get_recent_trading_days(tradingday, window_trading_days, timescale_crud)
        if not days:
            return pd.Series(dtype="float64"), (tradingday, tradingday), None, 0.1

        weekly_parts: List[pd.Series] = []
        ticks: List[float] = []

        main_contract = main_info["instrument_id"]
        sub_contract = sub_info["instrument_id"]

        for d in days:
            main_df = load_contract_data(main_contract, d, timescale_crud)
            main_df["high_limited"] = main_info["high_limited"]
            main_df["low_limited"] = main_info["low_limited"]

            sub_df = load_contract_data(sub_contract, d, timescale_crud)
            sub_df["high_limited"] = sub_info["high_limited"]
            sub_df["low_limited"] = sub_info["low_limited"]

            pre_main_df = load_contract_data(main_contract, d, timescale_crud, previous=True)
            pre_sub_df = load_contract_data(sub_contract, d, timescale_crud, previous=True)

            last_main_row = pre_main_df[pre_main_df["instrument_id"] == main_contract].tail(6).copy()
            main_df = pd.concat([last_main_row, main_df], ignore_index=True)
            last_sub_row = pre_sub_df[pre_sub_df["instrument_id"] == sub_contract].tail(6).copy()
            sub_df = pd.concat([last_sub_row, sub_df], ignore_index=True)

            main_c = str(main_df.iloc[0]["instrument_id"])
            sub_c  = str(sub_df.iloc[0]["instrument_id"])
            tick   = get_tick_size(main_c, timescale_crud) or 0.1
            ticks.append(float(tick))

            main_df = main_df[["instrument_id","trading_day","time","bid_price0","ask_price0","high_limited","low_limited"]]
            sub_df  = sub_df[["instrument_id","trading_day","time","bid_price0","ask_price0","high_limited","low_limited"]]

            main_df = remove_open_close_noise(main_df)
            sub_df  = remove_open_close_noise(sub_df)

            aligned = align_main_sub_minute(main_df, sub_df, float(tick))
            if aligned.empty or "spread" not in aligned.columns:
                continue

            _near, _far, sign = _is_main_near(main_c, sub_c)[1], _is_main_near(main_c, sub_c)[2], _is_main_near(main_c, sub_c)[3]
            spread_to_plot = aligned["spread"].astype(float) * int(sign)
            weekly_parts.append(spread_to_plot.dropna())

        weekly_spread = pd.concat(weekly_parts, ignore_index=True) if weekly_parts else pd.Series(dtype="float64")
        base_tick = float(min(ticks)) if ticks else 0.1

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

def make_histograms(spread_series, tick_size, weekly_spread=None, window_days=5):
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

# --------------------------------------------------------------------------------------
# 共享准备（被多个接口使用）
# --------------------------------------------------------------------------------------
def prepare_main_sub(args):
    p = _parse_params(args)
    product = p['product']
    trading_day = p['trading_day']
    sessions = p['sessions']
    is_manual = p['is_manual']
    near_ctp = p['near_ctp']
    far_ctp  = p['far_ctp']

    # 1) 加载数据
    if is_manual:
        systemLogger.info(f"[TODAY/prepare] mode=manual near={near_ctp} far={far_ctp} day={trading_day} sessions={sessions}")
        main_df, sub_df, main_info, sub_info = load_pair_data(timescale_crud, trading_day, near_ctp, far_ctp)
    else:
        systemLogger.info(f"[TODAY/prepare] mode=auto product={product} day={trading_day} sessions={sessions}")
        main_df, sub_df, main_info, sub_info = load_main_sub_data(timescale_crud, trading_day, product)

    if (main_df is None) or (sub_df is None) or main_df.empty or sub_df.empty:
        return None

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

    tick_size = float(get_tick_size(main_contract, timescale_crud) or 0.1)
    aligned_df = align_main_sub_minute(main_df, sub_df, tick_size)
    aligned_df, limit_timestamps = filter_spread_with_limit(aligned_df)
    if aligned_df.empty:
        return None

    spread = aligned_df['spread']
    spread_diff = spread.diff()
    spread_diff.index = aligned_df['time']

    leg1_diff = compute_leg_return_series(main_df)
    leg2_diff = compute_leg_return_series(sub_df)
    corr = compute_spread_leg_correlation(spread_diff, leg1_diff, leg2_diff)

    from tradeAssistantSpread import get_avg_volume
    main_avg = int(get_avg_volume(timescale_crud, main_contract, trading_day, days=30) or 0)
    sub_avg  = int(get_avg_volume(timescale_crud, sub_contract, trading_day, days=30) or 0)

    main_is_near, near_c, far_c, sign = _is_main_near(main_contract, sub_contract)
    spread_display = aligned_df['spread'].astype(float) * float(sign)

    prod_prefix = _prod_prefix(near_c) or _prod_prefix(far_c) or (product or '').upper()
    is_index = prod_prefix in {"IF", "IH", "IC", "IM"}
    def _slice_open_window(df: pd.DataFrame, is_index: bool) -> pd.DataFrame:
        if df is None or df.empty: return df
        open_start = "09:32" if is_index else "09:02"
        open_end   = "10:00" if is_index else "09:30"
        t = pd.to_datetime(df["time"])
        mask = (t.dt.strftime("%H:%M") >= open_start) & (t.dt.strftime("%H:%M") <= open_end)
        return df.loc[mask].copy()

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

# --------------------------------------------------------------------------------------
# 今日大接口：/spread/today/dashboard
# --------------------------------------------------------------------------------------
def today_dashboard(args):
    """
    路由薄层应直接：return today_dashboard(request.args)
    这里内部处理缓存命中与计算，返回 Flask Response（make_ok / make_err）
    """
    start_time = time.time()
    t0 = time.perf_counter()
    def _mark(tag): systemLogger.info(f"[PROF] {tag} +{(time.perf_counter()-t0)*1000:.1f}ms")

    p = _parse_params(args)
    product = p['product']
    trading_day = p['trading_day']
    sessions = p['sessions']
    group_size = p['group_size']
    near_ctp   = p['near_ctp']
    far_ctp    = p['far_ctp']
    is_manual  = p['is_manual']
    weekly_window_trading_days = p['weekly_days']

    _mark("enter")
    if not trading_day:
        return make_err(code=40001, message='missing trading_day', http_status=400)
    if not is_manual and not product:
        return make_err(code=40001, message='missing product', http_status=400)

    key = build_key(
        prefix="today:dashboard",
        params=dict(product=product, trading_day=trading_day, sessions=sessions,
                    group_size=group_size, near_ctp=near_ctp, far_ctp=far_ctp,
                    contract_mode=("manual" if is_manual else "auto"),
                    weekly_days=weekly_window_trading_days),
        include=["product","trading_day","sessions","group_size","near_ctp","far_ctp","contract_mode","weekly_days"]
    )
    cached = cache_get_bytes(key)
    if cached:
        return current_app.response_class(cached, mimetype="application/json")

    systemLogger.info(f"[TODAY] mode={'manual' if is_manual else 'auto'} product={product} day={trading_day} sessions={sessions}")
    _mark("after_params")

    # 加载主次
    if is_manual:
        main_df, sub_df, main_info, sub_info = load_pair_data(timescale_crud, trading_day, near_ctp, far_ctp)
    else:
        main_df, sub_df, main_info, sub_info = load_main_sub_data(timescale_crud, trading_day, product)

    ms = main_df.shape if isinstance(main_df, pd.DataFrame) else ('NA','NA')
    ss = sub_df.shape  if isinstance(sub_df,  pd.DataFrame) else ('NA','NA')
    systemLogger.info(f"[TODAY] raw shapes: main={ms}, sub={ss}")

    if (main_df is None) or (sub_df is None) or main_df.empty or sub_df.empty:
        return make_ok({"summary": None, "histogram": None, "trend": None}, message='no data: raw empty')

    main_contract = str(main_df.iloc[0]["instrument_id"])
    sub_contract  = str(sub_df.iloc[0]["instrument_id"])

    cols = ["instrument_id","trading_day","time","volume","bid_price0","ask_price0","high_limited","low_limited"]
    main_df = main_df[cols].copy()
    sub_df  = sub_df[cols].copy()

    trade_date = pd.to_datetime(trading_day).date()
    vol_main = int(main_df[main_df["trading_day"] == trade_date]["volume"].sum())
    vol_sub  = int(sub_df[sub_df["trading_day"] == trade_date]["volume"].sum())

    main_is_near, near_c, far_c, sign = _is_main_near(main_contract, sub_contract)

    # 开盘窗口切片辅助
    def _slice_open_window(df: pd.DataFrame, is_index: bool) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        open_start = "09:32" if is_index else "09:02"
        open_end   = "10:00" if is_index else "09:30"
        t = pd.to_datetime(df["time"])
        mask = (t.dt.strftime("%H:%M") >= open_start) & (t.dt.strftime("%H:%M") <= open_end)
        return df.loc[mask].copy()

    prod_prefix = (_prod_prefix(near_c) or _prod_prefix(far_c) or (product or '').upper())
    is_index = prod_prefix in {"IF", "IH", "IC", "IM"}

    open_main_df = _slice_open_window(remove_close_noise(main_df), is_index)
    open_sub_df  = _slice_open_window(remove_close_noise(sub_df),  is_index)

    def filter_by_sessions(df: pd.DataFrame) -> pd.DataFrame:
        return _filter_by_sessions(df, product, sessions)

    main_df = remove_open_close_noise(filter_by_sessions(main_df))
    sub_df  = remove_open_close_noise(filter_by_sessions(sub_df))

    tick_size = float(get_tick_size(main_contract, timescale_crud) or 0.1)
    aligned_df = align_main_sub_minute(main_df, sub_df, tick_size)
    aligned_df, limit_timestamps = filter_spread_with_limit(aligned_df)
    if aligned_df.empty:
        return make_ok({"summary": None, "histogram": None, "trend": None}, message='no data: aligned empty')

    spread = aligned_df['spread']
    spread_diff = spread.diff()
    spread_diff.index = aligned_df['time']

    # 单腿与相关
    leg1_diff = compute_leg_return_series(main_df)
    leg2_diff = compute_leg_return_series(sub_df)
    corr = compute_spread_leg_correlation(spread_diff, leg1_diff, leg2_diff)

    # 批量+缓存拿月均量
    avg_map = get_avg_volume_many_cached(timescale_crud, [main_contract, sub_contract], trading_day, days=30)
    main_avg = int(avg_map.get(main_contract, 0))
    sub_avg  = int(avg_map.get(sub_contract, 0))

    spread_display = aligned_df['spread'].astype(float) * float(sign)

    # 近 N 个交易日背景
    try:
        weekly_spread, weekly_range, hist_bins, _ = build_weekly_spread_samples(
            main_info=main_info,
            sub_info=sub_info,
            tradingday=pd.to_datetime(trading_day).date(),
            timescale_crud=timescale_crud,
            window_trading_days=weekly_window_trading_days
        )
    except Exception as _e:
        systemLogger.warning(f"weekly samples failed: {repr(_e)}")
        weekly_spread, weekly_range, hist_bins = None, None, None

    # 直方图
    hist_today, hist_weekly = make_histograms(
        spread_series=spread_display,
        tick_size=tick_size,
        weekly_spread=weekly_spread,
        window_days=weekly_window_trading_days
    )

    # 主趋势（每 group_size 分钟）
    df_trend = spread_diff.dropna().to_frame('diff')
    idx = pd.DatetimeIndex(df_trend.index)
    idx = idx.tz_localize('Asia/Shanghai') if idx.tz is None else idx.tz_convert('Asia/Shanghai')
    df_trend['bucket'] = idx.floor(f'{group_size}min')
    g = df_trend.groupby('bucket')['diff'].sum().reset_index()
    x = g['bucket'].dt.strftime('%H:%M').tolist()
    y = g['diff'].astype(float).round(6).tolist()
    if sign == -1:
        y = [float(v) * -1.0 for v in y]

    # 开盘趋势（可选）
    open_trend = None
    y_range_trend_open = None
    open_limit_timestamps = []
    if open_main_df is not None and not open_main_df.empty and open_sub_df is not None and not open_sub_df.empty:
        aligned_open_df = align_main_sub_minute(open_main_df, open_sub_df, float(tick_size))
        if not aligned_open_df.empty:
            aligned_open_df, open_limit_timestamps = filter_spread_with_limit(aligned_open_df)
            spread_open = aligned_open_df['spread']
            spread_open_diff = spread_open.diff()
            spread_open_diff.index = aligned_open_df['time']
            open_exch_times = aligned_open_df['time']

            times_open = pd.to_datetime(open_exch_times)
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

            # 与主趋势统一 y 轴范围
            y2 = np.asarray(y, dtype=float)
            y3 = np.asarray(open_y_vals, dtype=float)
            valid2 = y2[np.isfinite(y2)]
            valid3 = y3[np.isfinite(y3)]
            if valid2.size + valid3.size > 0:
                all_vals = np.concatenate([valid2, valid3])
                max_abs = np.max(np.abs(all_vals))
                max_abs = max(max_abs, 1e-6)
                pad = max_abs * 0.08
                y_min, y_max = float(-max_abs - pad), float(max_abs + pad)
                y_range_trend_open = {"min": y_min, "max": y_max}

    spread_total_change_display = float((spread_diff.dropna().sum() or 0.0) * sign)

    summary = {
      "product": product.upper(),
      "trading_day": trading_day,
      "tick_size": float(tick_size),
      "main_contract": main_contract,
      "sub_contract": sub_contract,
      "near_contract": near_c,
      "far_contract": far_c,
      "main_limit_flag": bool(aligned_df["main_limited_flag"].any()),
      "sub_limit_flag": bool(aligned_df["sub_limited_flag"].any()),
      "vol_main": int(vol_main),
      "vol_sub": int(vol_sub),
      "main_avg_month_volume": int(main_avg),
      "sub_avg_month_volume": int(sub_avg),
      "leg1_total_change": float(compute_leg_volatility(leg1_diff) * 0.0 + leg1_diff.dropna().sum() if True else float(0.0)),
      "leg2_total_change": float(compute_leg_volatility(leg2_diff) * 0.0 + leg2_diff.dropna().sum() if True else float(0.0)),
      "spread_total_change": float(spread_diff.dropna().sum() or 0.0),
      "spread_total_change_display": float(spread_total_change_display),
      "leg1_volatility": float(compute_leg_volatility(leg1_diff) or 0.0),
      "leg2_volatility": float(compute_leg_volatility(leg2_diff) or 0.0),
      "spread_volatility": float(compute_spread_volatility(spread_diff) or 0.0),
      "corr_spread_leg1": float(corr["spread_vs_leg1"] or 0.0),
      "corr_spread_leg2": float(corr["spread_vs_leg2"] or 0.0),
      "corr_leg1_leg2": float(corr["leg1_vs_leg2"] or 0.0)
    }

    resp = make_ok({
        "summary": summary,
        "histogram": hist_today,
        "histogram_weekly": hist_weekly,
        "trend": {"x": x, "y": y},
        "open_trend": open_trend,
        "y_range_trend_open": y_range_trend_open
    })

    try:
        ttl = ttl_by_trading_day(trading_day, hot_ttl=30, cold_ttl=24*3600)
        cache_set_bytes(key, resp.get_data(), ttl)
    except Exception:
        pass

    systemLogger.info(f"[TODAY] elapsed={time.time() - start_time:.3f}s")
    return resp



def history_dashboard(args) -> Any:
    """
    历史仪表盘占位实现：
    与 /spread/history/dashboard 的路由输出结构对齐，字段留空。
    """
    product = (args.get("product") or "").upper()
    start   = args.get("start") or ""
    end     = args.get("end") or ""
    sessions = (args.get("sessions") or "all").lower()
    auto_contract = not ((args.get("near_contract") or "") and (args.get("far_contract") or ""))

    if not start or not end:
        return make_err(code=40001, message="start/end 参数必传", http_status=400)

    payload = {
        "x": [],
        "contracts": [],
        "metrics": {
            "vol_near": [],
            "vol_far": [],
            "dP_near": [],
            "dP_far": [],
            "volat_near": [],
            "volat_far": [],
            "spread_delta": [],
            "spread_volat": [],
            "corr_spread_near": [],
            "corr_spread_far": [],
        },
        "summary": {
            "days": 0,
            "skipped": 0,
            "avg_vol_near": None,
            "avg_vol_far": None,
            "avg_volat_spread": None,
        },
        "params": {
            "product": product,
            "start": start,
            "end": end,
            "sessions": sessions,
            "auto_contract": bool(auto_contract),
            "near_contract": args.get("near_contract") or "",
            "far_contract": args.get("far_contract") or "",
        },
    }
    return make_ok(payload, message="stub: history_dashboard")


def get_contracts(args) -> Any:
    """
    合约列表占位实现：
    与 /spread/contracts 对齐。仅检查必填并返回空列表。
    """
    product = (args.get("product") or "").strip()
    if not product:
        return make_err(code=40001, message="product 必传", http_status=400)

    on_date = (args.get("on") or "")
    payload = {
        "product": product,
        "on": on_date,
        "options": [],
        "default_near": "",
        "default_far": "",
    }
    return make_ok(payload, message="stub: contracts")


# ========== 下方是“今天页面”会复用的函数占位 ==========
def prepare_main_sub(args) -> Optional[Dict[str, Any]]:
    """
    供 /spread/today/* 路由复用的上下文准备函数（占位）。
    路由会优雅处理 None，先保证能跑通。
    """
    return None


def make_histograms(
    spread_series,
    tick_size: float,
    weekly_spread=None,
    window_days: int = 5,
) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    """
    直方图占位：返回空桶。
    """
    return {"bins": [], "counts": []}, None
