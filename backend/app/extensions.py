# app/extensions.py
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from common.logger import systemLogger
from tools.timescaleManager import TimescaleCRUD
import redis

db = SQLAlchemy()
cors = CORS()

ts_crud = None
timescale_crud = None
rds = None

def init_timescale(app):
    global ts_crud, timescale_crud
    url = app.config["TIMESCALE_URL"]
    ts_crud = TimescaleCRUD(url, pool_size=20, max_overflow=0)
    timescale_crud = TimescaleCRUD(url)

def init_redis(app):
    global rds
    rds = redis.from_url(app.config["REDIS_URL"], decode_responses=False)
