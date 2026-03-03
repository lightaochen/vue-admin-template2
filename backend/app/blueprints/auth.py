# app/blueprints/auth.py
from flask import Blueprint, request
from werkzeug.security import generate_password_hash, check_password_hash
from ..extensions import db
from ..models.user import User
from ..utils.response import make_ok, make_err
from ..utils.jwt_utils import generate_token, verify_token

bp = Blueprint("auth", __name__)

@bp.route("/login", methods=["POST", "OPTIONS"])
def login():
    if request.method == "OPTIONS": return ("", 204)
    body = request.get_json(silent=True) or {}
    username = (body.get("username") or "").strip()
    password = body.get("password") or ""
    user = User.query.filter_by(username=username).first()
    if not user: return make_err(code=40101, message="Username error", http_status=401)
    if not check_password_hash(user.password_hash, password):
        return make_err(code=40101, message="Password error", http_status=401)
    return make_ok({"token": generate_token(username)})

@bp.route("/info", methods=["GET", "OPTIONS"])
def info():
    if request.method == "OPTIONS": return ("", 204)
    token = request.headers.get("X-Token") or request.args.get("token")
    if not token:
        auth = request.headers.get("Authorization", "")
        if auth.lower().startswith("bearer "):
            token = auth.split(" ", 1)[1].strip()
    if not token: return make_err(code=50008, message="Missing token", http_status=401)
    payload, err = verify_token(token)
    if err: msg, code = err; return make_err(code=code, message=msg, http_status=401)
    user = User.query.filter_by(username=payload.get("sub")).first()
    if not user: return make_err(code=50008, message="User not found", http_status=401)
    return make_ok({"name": user.name, "avatar": user.avatar, "roles": user.roles})

@bp.route("/logout", methods=["POST", "OPTIONS"])
def logout():
    if request.method == "OPTIONS": return ("", 204)
    return make_ok(None)
