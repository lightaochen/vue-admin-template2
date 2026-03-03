# wsgi.py（部署用）
from app import create_app
app = create_app("production")
