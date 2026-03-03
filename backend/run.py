from dotenv import load_dotenv
load_dotenv()  # 读取 .env
from app import create_app
from app.extensions import db
from app.models.models import User   # 你的 User 模型

app = create_app()

# 首次启动初始化用户表（如果你仍在 MySQL里放 User）
with app.app_context():
    db.create_all()
    # 自动创建管理员 admin / 111111（按需保留/修改）
    from werkzeug.security import generate_password_hash
    if not User.query.filter_by(username="admin").first():
        u = User(username="admin", name="Admin", password_hash=generate_password_hash("111111"), roles_json='["admin"]')
        db.session.add(u)
        db.session.commit()

if __name__ == "__main__":
    # 本地开发
    app.run(host="0.0.0.0", port=8080, debug=True)
