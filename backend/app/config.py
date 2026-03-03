# app/config.py
import os

class git_test:
    aaaaaa = 1


class BaseConfig:
    JSON_AS_ASCII = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = os.getenv("SECRET_KEY", "change-me")
    TOKEN_EXPIRE_SECONDS = int(os.getenv("TOKEN_EXPIRE_SECONDS", 7200))
    # MySQL（用户/权限）
    SQLALCHEMY_DATABASE_URI = os.getenv(
        "DB_URI",
        "mysql+pymysql://ta_admin:StrongPassword123!@127.0.0.1:3306/ta_admin?charset=utf8mb4"
    )
    # Timescale / Postgres（行情）
    TIMESCALE_URL = os.getenv("DATABASE_URL")  # 你原有的
    # Redis
    REDIS_URL = os.getenv("REDIS_URL", "redis://:avg123@localhost:6379/0")
    CACHE_VER = os.getenv("CACHE_VER", "v1")
    # SQLAlchemy engine options
    SQLALCHEMY_ENGINE_OPTIONS = {"pool_pre_ping": True, "pool_recycle": 280}

class DevelopmentConfig(BaseConfig):
    DEBUG = True

class ProductionConfig(BaseConfig):
    DEBUG = False

config_by_name = {
    "development": DevelopmentConfig,
    "production": ProductionConfig,
}
