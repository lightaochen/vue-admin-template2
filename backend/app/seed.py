# app/seed.py
from werkzeug.security import generate_password_hash
from .extensions import db
from .models.user import User

def init_db_and_seed():
    db.create_all()
    if not User.query.filter_by(username="admin").first():
        u = User(username="admin", name="Admin", roles=["admin"],
                 password_hash=generate_password_hash("111111"))
        db.session.add(u); db.session.commit()
