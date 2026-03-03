# app/services/contract_utils.py
"""
合约工具函数模块
提供合约相关的工具函数，如获取tick_size、产品前缀、判断近远月等
"""
from typing import Optional, Tuple
import pandas as pd
import numpy as np
from datetime import datetime

from models.models import ChinaFuturesBaseInfo
from tools.timescaleManager import TimescaleCRUD
from config.settings import TRADING_SESSION_RULES
from common.logger import systemLogger


def get_product_prefix(instrument_id: str) -> str:
    """
    从合约代码中提取品种前缀。

    参数:
        instrument_id (str): 合约代码，如 'CU2509'

    返回:
        str: 品种前缀（小写），如 'cu'
    """
    # 逐字符遍历，找到第一个非字母位置，然后切片
    for i, ch in enumerate(instrument_id):
        if not ch.isalpha():
            return instrument_id[:i].lower()
    return instrument_id.lower()  # 如果全是字母，返回全部


def get_tick_size(contract_id: str, timescale_crud: TimescaleCRUD) -> Optional[float]:
    """
    获取某合约的最小变动单位（tick_size）。

    参数:
        contract_id (str): 合约代码
        timescale_crud (TimescaleCRUD): 数据库接口对象

    返回:
        float or None: tick_size 数值，若未找到则返回 None
    """
    try:
        with timescale_crud.session_scope() as session:
            result = session.query(ChinaFuturesBaseInfo.tick_size)\
                .filter(ChinaFuturesBaseInfo.instrument_id.ilike(contract_id))\
                .limit(1)\
                .first()

            if result and result.tick_size is not None:
                tick = float(result.tick_size)
                systemLogger.info(
                    f"✅ Tick Size 获取成功: 合约={contract_id}, tick_size={tick}"
                )
                return tick
            else:
                systemLogger.warning(f"⚠️ Tick Size 未找到: 合约={contract_id}")
                return None

    except Exception as e:
        systemLogger.exception(f"❌ Tick Size 查询失败: 合约={contract_id}, 错误: {e}")
        return None


def parse_contract_yyyymm(instrument_id: str) -> Optional[int]:
    """
    从合约代码中解析出年月（YYYYMM格式）。

    参数:
        instrument_id (str): 合约代码，如 'CU2509'

    返回:
        int or None: 年月，如 202509，无法解析返回 None
    """
    if not instrument_id:
        return None
    import re
    m = re.search(r'(\d{4,5})$', instrument_id)
    if not m:
        return None
    yy = m.group(1)
    try:
        y = 2000 + int(yy[:2])
        mth = int(yy[2:])
        return y * 100 + mth
    except:
        return None


def is_main_near(
    main_contract: str,
    sub_contract: str
) -> Tuple[bool, str, str, int]:
    """
    判断主力合约是否为近月，并返回近月、远月合约代码和符号。

    参数:
        main_contract (str): 主力合约代码
        sub_contract (str): 次主力合约代码

    返回:
        (is_near, near_contract, far_contract, sign):
            - is_near: 主力是否为近月
            - near_contract: 近月合约代码
            - far_contract: 远月合约代码
            - sign: 展示符号（1或-1），用于统一展示为"近月-远月"
    """
    main_ym = parse_contract_yyyymm(main_contract) or 999999
    sub_ym = parse_contract_yyyymm(sub_contract) or 999999

    if main_ym <= sub_ym:
        # 主力是近月
        return True, main_contract, sub_contract, 1
    else:
        # 主力是远月，需要取反
        return False, sub_contract, main_contract, -1


def remove_open_close_noise(df: pd.DataFrame) -> pd.DataFrame:
    """
    根据合约品种规则，剔除不在交易时间段内的数据。

    参数:
        df: 包含 'instrument_id' 和 'time' 列的 DataFrame

    返回:
        过滤后的 DataFrame，移除非交易时间数据
    """
    if df.empty:
        systemLogger.warning("⚠️ 输入数据为空，跳过交易时段过滤")
        return df

    try:
        df = df.copy()
        df["__time_only__"] = pd.to_datetime(df["time"], errors="coerce").dt.time
        df["__product__"] = df["instrument_id"].apply(get_product_prefix)

        def is_in_valid_session(row):
            product = row["__product__"]
            t = row["__time_only__"]
            if pd.isnull(t):
                return False
            sessions = TRADING_SESSION_RULES.get(product, [])
            for s in sessions:
                start = datetime.strptime(s[0], "%H:%M").time()
                end = datetime.strptime(s[1], "%H:%M").time()
                if start <= end:
                    if start <= t <= end:
                        return True
                else:
                    if t >= start or t <= end:
                        return True
            return False

        before_rows = len(df)
        df = df[df.apply(is_in_valid_session, axis=1)].copy()
        after_rows = len(df)

        systemLogger.info(
            f"🧹 交易时段过滤: 原始={before_rows} 行，保留={after_rows} 行，"
            f"过滤={before_rows - after_rows} 行"
        )
        return df.drop(columns=["__time_only__", "__product__"])

    except Exception as e:
        systemLogger.exception(f"❌ 交易时段过滤失败: 错误={e}")
        return df
