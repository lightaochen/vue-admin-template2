# app/blueprints/spread_today.py
from __future__ import annotations

from flask import Blueprint, request
import numpy as np
import pandas as pd

from app.services.spread_service import (
    today_dashboard,
    prepare_main_sub,
    make_histograms,
)
from app.utils.response import make_ok, make_err
from common.logger import systemLogger

bp_spread_today = Blueprint("spread_today", __name__, url_prefix="/spread/today")


@bp_spread_today.get("/dashboard")
def route_dashboard():
    """
    完整仪表盘（含 summary / histogram / trend / open_trend）
    绝大多数计算在 service.today_dashboard 中完成
    """
    try:
        # 直接转发给 service，返回即是 Flask Response
        return today_dashboard(request.args)
    except Exception as e:
        systemLogger.exception(f"/spread/today/dashboard failed: {e}")
        return make_err(code=50000, message="server error", http_status=500)


@bp_spread_today.get("/summary")
def route_summary():
    """
    轻量 Summary：调用 service.prepare_main_sub 获取上下文并组装 summary 字段
    """
    try:
        ctx = prepare_main_sub(request.args)
        if ctx is None:
            return make_ok({"summary": None}, message="no data")

        spread_total_change_display = float(
            (ctx["spread_diff"].dropna().sum() or 0.0) * ctx["sign"]
        )

        summary = {
            "product": ctx["product"],
            "trading_day": request.args.get("trading_day", ""),
            "tick_size": float(ctx["tick_size"]),
            "main_contract": ctx["main_contract"],
            "sub_contract": ctx["sub_contract"],
            "near_contract": ctx["near_c"],
            "far_contract": ctx["far_c"],
            "main_limit_flag": bool(ctx["aligned_df"]["main_limited_flag"].any()),
            "sub_limit_flag": bool(ctx["aligned_df"]["sub_limited_flag"].any()),
            "vol_main": int(ctx["vol_main"]),
            "vol_sub": int(ctx["vol_sub"]),
            "main_avg_month_volume": int(ctx["main_avg"]),
            "sub_avg_month_volume": int(ctx["sub_avg"]),
            "leg1_total_change": float(ctx["leg1_diff"].dropna().sum() or 0.0),
            "leg2_total_change": float(ctx["leg2_diff"].dropna().sum() or 0.0),
            "spread_total_change": float(ctx["spread_diff"].dropna().sum() or 0.0),
            "spread_total_change_display": spread_total_change_display,
            "leg1_volatility": float(ctx["leg1_diff"].dropna().std() or 0.0),
            "leg2_volatility": float(ctx["leg2_diff"].dropna().std() or 0.0),
            "spread_volatility": float(ctx["spread_diff"].dropna().std() or 0.0),
            "corr_spread_leg1": float(ctx["corr"]["spread_vs_leg1"] or 0.0),
            "corr_spread_leg2": float(ctx["corr"]["spread_vs_leg2"] or 0.0),
            "corr_leg1_leg2": float(ctx["corr"]["leg1_vs_leg2"] or 0.0),
        }
        return make_ok({"summary": summary})
    except Exception as e:
        systemLogger.exception(f"/spread/today/summary failed: {e}")
        return make_ok({"summary": None}, message="error")


@bp_spread_today.get("/histogram")
def route_histogram():
    """
    当日直方图：与大接口保持一致的桶生成逻辑（此处默认不叠加近N日背景）
    """
    try:
        weekly_days = int(request.args.get("weekly_days") or 5)  # 仅用于回填字段
        ctx = prepare_main_sub(request.args)
        if ctx is None:
            return make_ok({"histogram": None}, message="no data")

        hist, _ = make_histograms(
            spread_series=ctx["spread_series"],
            tick_size=ctx["tick_size"],
            weekly_spread=None,
            window_days=weekly_days,
        )
        return make_ok({"histogram": hist})
    except Exception as e:
        systemLogger.exception(f"/spread/today/histogram failed: {e}")
        return make_ok({"histogram": None}, message="error")


@bp_spread_today.get("/trend")
def route_trend():
    """
    主趋势：按 group_size 分钟聚合 spread_diff 之和；展示口径为“近-远”
    """
    try:
        group_size = int(request.args.get("group_size") or 10)
        ctx = prepare_main_sub(request.args)
        if ctx is None:
            return make_ok({"trend": None}, message="no data")

        df_trend = ctx["spread_diff"].dropna().to_frame("diff")
        idx = pd.DatetimeIndex(df_trend.index)
        idx = (
            idx.tz_localize("Asia/Shanghai")
            if idx.tz is None
            else idx.tz_convert("Asia/Shanghai")
        )
        df_trend["bucket"] = idx.floor(f"{group_size}min")
        g = df_trend.groupby("bucket")["diff"].sum().reset_index()

        x = g["bucket"].dt.strftime("%H:%M").tolist()
        y = g["diff"].astype(float).round(6).tolist()
        if ctx["sign"] == -1:
            y = [float(v) * -1.0 for v in y]

        return make_ok({"trend": {"x": x, "y": y}})
    except Exception as e:
        systemLogger.exception(f"/spread/today/trend failed: {e}")
        return make_ok({"trend": None}, message="error")


@bp_spread_today.get("/open_trend")
def route_open_trend():
    """
    开盘窗口趋势（股指 09:32–10:00 / 其余 09:02–09:30），并返回建议 y 轴范围
    """
    try:
        ctx = prepare_main_sub(request.args)
        if ctx is None:
            return make_ok(
                {"open_trend": None, "y_range_trend_open": None}, message="no data"
            )

        open_trend = None
        y_range_trend_open = None

        if (
            ctx["open_main_df"] is not None
            and not ctx["open_main_df"].empty
            and ctx["open_sub_df"] is not None
            and not ctx["open_sub_df"].empty
        ):
            from tradeAssistantSpread import align_main_sub_minute, filter_spread_with_limit

            aligned_open_df = align_main_sub_minute(
                ctx["open_main_df"], ctx["open_sub_df"], float(ctx["tick_size"])
            )
            if not aligned_open_df.empty:
                aligned_open_df, open_limit_ts = filter_spread_with_limit(
                    aligned_open_df
                )
                spread_open = aligned_open_df["spread"]
                spread_open_diff = spread_open.diff()
                spread_open_diff.index = aligned_open_df["time"]
                times_open = pd.to_datetime(aligned_open_df["time"])

                s_open = pd.Series(
                    spread_open_diff, index=pd.to_datetime(spread_open_diff.index)
                )
                s_aligned = s_open.reindex(times_open)
                open_y_vals = (
                    s_aligned.fillna(0).astype(float) * float(ctx["sign"])
                ).tolist()
                open_x_labels = pd.Series(times_open).dt.strftime("%H:%M").tolist()

                open_trend = {
                    "x": open_x_labels,
                    "y": open_y_vals,
                    "limit_ts": [
                        pd.to_datetime(ts).strftime("%Y-%m-%d %H:%M:%S")
                        for ts in (open_limit_ts or [])
                    ],
                    "window": {
                        "start": "09:32" if ctx["is_index"] else "09:02",
                        "end": "10:00" if ctx["is_index"] else "09:30",
                    },
                    "is_index": bool(ctx["is_index"]),
                }

                # 计算建议的 y 轴范围（±max_abs 并留 8% padding）
                vals = np.asarray(open_y_vals, dtype=float)
                vals = vals[np.isfinite(vals)]
                if vals.size > 0:
                    max_abs = max(np.max(np.abs(vals)), 1e-6)
                    pad = max_abs * 0.08
                    y_range_trend_open = {
                        "min": float(-max_abs - pad),
                        "max": float(max_abs + pad),
                    }

        return make_ok(
            {"open_trend": open_trend, "y_range_trend_open": y_range_trend_open}
        )
    except Exception as e:
        systemLogger.exception(f"/spread/today/open_trend failed: {e}")
        return make_ok(
            {"open_trend": None, "y_range_trend_open": None}, message="error"
        )
