"""
Flask 应用工厂
"""
from flask import Flask
from app.extensions import db, cors, init_redis, init_timescale
from app.blueprints.spread_today import bp_spread_today
from app.blueprints.spread_history import bp_spread_history
from app.blueprints.contracts import bp_contracts
from app.blueprints.auth import bp as bp_auth
from app.blueprints.pnl import bp_pnl
from app.utils.response import register_error_handlers
from config.settings import DB_URI, DATABASE_URL, REDIS_URL


def create_app():
    app = Flask(__name__)

    app.config["JSON_AS_ASCII"] = False
    app.config["SQLALCHEMY_DATABASE_URI"] = DB_URI
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
        "pool_pre_ping": True,
        "pool_recycle": 280
    }

    app.config["CACHE_VER"] = "v1"

    import os
    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "change-me-to-a-random-secret")
    app.config["TOKEN_EXPIRE_SECONDS"] = 2 * 60 * 60

    db.init_app(app)
    cors.init_app(
        app,
        resources={r"/*": {"origins": "*"}},
        supports_credentials=False,
        methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Content-Type", "X-Token", "Authorization"]
    )

    init_timescale(app)
    init_redis(app)

    app.register_blueprint(bp_spread_today)
    app.register_blueprint(bp_spread_history)
    app.register_blueprint(bp_contracts)
    app.register_blueprint(bp_auth, url_prefix="/user")
    app.register_blueprint(bp_pnl)

    register_error_handlers(app)

    return app
