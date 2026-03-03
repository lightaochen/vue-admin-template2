# app/services/calculations.py
"""
计算服务模块
提供价差计算、波动率计算、相关性计算等核心算法
"""
from typing import Tuple, List
import pandas as pd
import numpy as np

from common.logger import systemLogger


def align_main_sub_minute(
    main_df: pd.DataFrame,
    sub_df: pd.DataFrame,
    tick_size: float
) -> pd.DataFrame:
    """
    对齐主力与次主力分钟数据，并计算中间价、价差（spread）。

    参数:
        main_df: 主力合约数据，含 time, bid/ask_price0
        sub_df: 次主力合约数据
        tick_size: 最小跳动单位

    返回:
        pd.DataFrame: 包含 mid_main, mid_sub, spread 的对齐结果
    """
    try:
        time_main = set(main_df["time"])
        time_sub = set(sub_df["time"])
        common_times = sorted(list(time_main & time_sub))
        all_times = sorted(list(time_main | time_sub))

        systemLogger.info(
            f"⌛ 时间戳对齐检查: 主力={len(time_main)} 次主力={len(time_sub)} "
            f"共有={len(common_times)} 总计={len(all_times)}"
        )

        if len(common_times) == 0:
            systemLogger.warning("⚠️ 主力与次主力时间戳无交集，无法对齐")
            return pd.DataFrame()

        # 合并数据（仅保留交集）
        df = pd.merge(main_df, sub_df, on="time", suffixes=("_main", "_sub"))
        systemLogger.info(f"✅ 合并成功: 行数={len(df)}")

        # 中间价计算（强制转 float）
        df["mid_main"] = (
            (df["bid_price0_main"].astype(float) + df["ask_price0_main"].astype(float)) / 2
        )
        df["mid_sub"] = (
            (df["bid_price0_sub"].astype(float) + df["ask_price0_sub"].astype(float)) / 2
        )

        # Spread 计算（使用 float tick_size）
        raw_spread = df["mid_main"] - df["mid_sub"]
        df["spread"] = np.ceil(raw_spread / float(tick_size)) * float(tick_size)

        systemLogger.debug(
            f"价差样本: spread.head() = {df['spread'].head().tolist()}"
        )

        return df

    except Exception as e:
        systemLogger.exception(f"❌ 对齐分钟数据失败: 错误={e}")
        return pd.DataFrame()


def filter_spread_with_limit(
    df: pd.DataFrame
) -> Tuple[pd.DataFrame, List]:
    """
    过滤涨跌停时段的数据，并标记涨跌停标志。

    参数:
        df: 包含 high_limited, low_limited, mid_main, mid_sub 的 DataFrame

    返回:
        (filtered_df, limit_timestamps): 过滤后的数据和涨跌停时间戳列表
    """
    try:
        if df.empty:
            return df, []

        df = df.copy()

        # 判断主力是否涨跌停
        is_limit_main = (
            (df["mid_main"].astype(float) >= df["high_limited_main"].astype(float)) |
            (df["mid_main"].astype(float) <= df["low_limited_main"].astype(float))
        )
        df["main_limited_flag"] = is_limit_main

        # 判断次主力是否涨跌停
        is_limit_sub = (
            (df["mid_sub"].astype(float) >= df["high_limited_sub"].astype(float)) |
            (df["mid_sub"].astype(float) <= df["low_limited_sub"].astype(float))
        )
        df["sub_limited_flag"] = is_limit_sub

        # 过滤掉任一合约涨跌停的数据
        mask = ~(is_limit_main | is_limit_sub)
        df_filtered = df[mask].copy()

        # 收集涨跌停时间戳
        limit_timestamps = df[~mask]["time"].tolist() if not df[~mask].empty else []

        systemLogger.info(
            f"🚫 涨跌停屏蔽: 主力={is_limit_main.sum()} 分钟, "
            f"次主力={is_limit_sub.sum()} 分钟, 总计={len(limit_timestamps)}"
        )
        return df_filtered, limit_timestamps

    except Exception as e:
        systemLogger.exception(f"❌ 涨跌停过滤失败: 错误={e}")
        return df, []


def compute_leg_return_series(df: pd.DataFrame) -> pd.Series:
    """
    计算合约中间价序列的分钟变化值（差分序列）。

    参数:
        df: 包含 'bid_price0', 'ask_price0', 'time' 列的 DataFrame

    返回:
        pd.Series: 差分序列，index = time, dtype=float64
    """
    try:
        if df.empty or "bid_price0" not in df or "ask_price0" not in df or "time" not in df:
            raise ValueError("输入数据缺少必要字段或为空")

        df = df.copy()
        df.set_index("time", inplace=True)

        # 关键：把 Decimal/object 转成 float
        bid = pd.to_numeric(df["bid_price0"], errors="coerce").astype(float)
        ask = pd.to_numeric(df["ask_price0"], errors="coerce").astype(float)

        mid_price = (bid + ask) / 2.0
        diff_series = mid_price.diff()  # 第一条会是 NaN 正常

        return diff_series

    except Exception as e:
        systemLogger.exception(f"❌ compute_leg_return_series 计算失败: 错误={e}")
        return pd.Series(dtype="float64")


def compute_leg_volatility(leg_diff: pd.Series) -> float:
    """
    计算单边腿价格变化序列的波动率（标准差）。

    参数:
        leg_diff: 差分序列

    返回:
        float: 标准差值
    """
    try:
        cleaned = leg_diff.dropna().iloc[1:].astype(float)
        if cleaned.empty:
            systemLogger.warning("⚠️ 差分序列为空，无法计算波动率")
            return 0.0

        std_val = cleaned.std(ddof=1)
        systemLogger.info(
            f"📉 单腿波动率计算: 波动率={std_val:.6f}, 样本数={len(cleaned)}"
        )
        return float(std_val)

    except Exception as e:
        systemLogger.exception(f"❌ compute_leg_volatility 计算失败: 错误={e}")
        return 0.0


def compute_spread_volatility(spread_diff: pd.Series) -> float:
    """
    计算价差变动序列的波动率（样本标准差），跳过第一条（跨日差值）。

    参数:
        spread_diff: 差分后的价差序列

    返回:
        float: 波动率（标准差）
    """
    try:
        cleaned = spread_diff.dropna().iloc[1:]
        if cleaned.empty:
            systemLogger.warning("⚠️ 价差差分序列为空，无法计算波动率")
            return 0.0

        std_val = cleaned.std(ddof=1)
        systemLogger.info(
            f"📊 价差波动率计算成功: 波动率={std_val:.6f}, 样本数={len(cleaned)}"
        )
        return float(std_val)

    except Exception as e:
        systemLogger.exception(f"❌ compute_spread_volatility 计算失败: 错误={e}")
        return 0.0


def compute_spread_leg_correlation(
    spread_diff: pd.Series,
    leg1_diff: pd.Series,
    leg2_diff: pd.Series
) -> dict:
    """
    计算价差与腿1/腿2、腿1与腿2的相关性；对齐索引，少于2个样本返回0。

    参数:
        spread_diff: 价差差分序列
        leg1_diff: 腿1差分序列
        leg2_diff: 腿2差分序列

    返回:
        dict: 包含三个相关性值的字典
    """
    try:
        s = pd.to_numeric(spread_diff, errors="coerce")
        l1 = pd.to_numeric(leg1_diff, errors="coerce")
        l2 = pd.to_numeric(leg2_diff, errors="coerce")

        def pair_corr(a: pd.Series, b: pd.Series) -> float:
            df = pd.concat([a, b], axis=1, join="inner").dropna()
            if df.shape[0] < 2:
                return 0.0
            c = df.iloc[:, 0].corr(df.iloc[:, 1])
            try:
                from math import isfinite
                return float(c) if (c is not None and isfinite(float(c))) else 0.0
            except Exception:
                return 0.0

        return {
            "spread_vs_leg1": pair_corr(s, l1),
            "spread_vs_leg2": pair_corr(s, l2),
            "leg1_vs_leg2": pair_corr(l1, l2),
        }

    except Exception as e:
        systemLogger.exception(
            f"❌ compute_spread_leg_correlation 计算失败(稳健版): 错误={e}"
        )
        return {"spread_vs_leg1": 0.0, "spread_vs_leg2": 0.0, "leg1_vs_leg2": 0.0}
