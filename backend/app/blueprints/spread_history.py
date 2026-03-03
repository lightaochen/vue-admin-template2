# app/blueprints/spread_history.py
from __future__ import annotations

from flask import Blueprint, request
from app.services.spread_service import history_dashboard  # 由 service 层实现
from app.utils.response import make_ok, make_err
from common.logger import systemLogger

bp_spread_history = Blueprint("spread_history", __name__, url_prefix="/spread/history")


@bp_spread_history.get("/dashboard")
def route_history_dashboard():
    """
    历史仪表盘（带缓存与子缓存）。
    业务逻辑全部在 services.spread_service.history_dashboard 中。
    必填参数：start, end
    可选参数：product, sessions, near_contract, far_contract, debug
    """
    try:
        return history_dashboard(request.args)
    except Exception as e:
        systemLogger.exception(f"/spread/history/dashboard failed: {e}")
        return make_err(code=50099, message="history dashboard error", http_status=500)
