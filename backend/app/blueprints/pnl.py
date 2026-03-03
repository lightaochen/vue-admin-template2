# app/blueprints/pnl.py
from flask import Blueprint
from app.utils.response import make_ok, make_err

bp_pnl = Blueprint("pnl", __name__, url_prefix="/pnl")

@bp_pnl.route("/data", methods=["GET"])
def pnl_data():
    return make_ok({"message": "pnl data endpoint"})
