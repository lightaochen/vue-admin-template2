# app/models/user.py
import json
from ..extensions import db

class User(db.Model):
    __tablename__ = "users"
    id            = db.Column(db.Integer, primary_key=True)
    username      = db.Column(db.String(80), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    name          = db.Column(db.String(80), nullable=False, default="")
    avatar        = db.Column(db.String(255), nullable=False, default="")
    roles_json    = db.Column(db.Text, nullable=False, default="[]")

    @property
    def roles(self):
        try:
            return json.loads(self.roles_json or "[]")
        except Exception:
            return []

    @roles.setter
    def roles(self, value):
        self.roles_json = json.dumps(value or [])
