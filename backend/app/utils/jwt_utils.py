# app/utils/jwt_utils.py
from datetime import datetime, timedelta
import jwt
from flask import current_app

def generate_token(username: str) -> str:
    now = datetime.utcnow()
    payload = {"sub": username, "iat": now,
               "exp": now + timedelta(seconds=current_app.config["TOKEN_EXPIRE_SECONDS"])}
    return jwt.encode(payload, current_app.config["SECRET_KEY"], algorithm="HS256")

def verify_token(token: str):
    try:
        payload = jwt.decode(token, current_app.config["SECRET_KEY"], algorithms=["HS256"])
        return payload, None
    except jwt.ExpiredSignatureError:
        return None, ("Token expired", 50014)
    except Exception:
        return None, ("Invalid token", 50008)
