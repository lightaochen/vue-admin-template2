# app/blueprints/contracts.py
from __future__ import annotations

from flask import Blueprint, request
from app.services.spread_service import contracts_options
from app.utils.response import make_ok, make_err
from common.logger import systemLogger

bp_contracts = Blueprint("contracts", __name__, url_prefix="/spread")


@bp_contracts.get("/contracts")
def route_contracts():
    try:
        return contracts_options(request.args)
    except Exception as e:
        systemLogger.exception(f"/spread/contracts failed: {e}")
        return make_err(code=50099, message="contracts error", http_status=500)
