import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from contextlib import contextmanager

p = 'MarketDataSystem'
rootPath = os.path.join(os.path.abspath(__file__).split(p)[0], p)
if rootPath not in sys.path:
    sys.path.append(rootPath)

from models.models import Base

class TimescaleCRUD:
    def __init__(self, db_url: str, pool_size=20, max_overflow=0):
        """优化连接池配置"""
        self.engine = create_engine(
            db_url,
            pool_size=pool_size,  # 连接池大小
            max_overflow=max_overflow,  # 最大溢出连接数
            pool_pre_ping=True  # 自动检测失效连接
        )
        self.session_factory = sessionmaker(bind=self.engine)
        self.ScopedSession = scoped_session(self.session_factory)
        Base.metadata.create_all(self.engine)
    
    @contextmanager
    def session_scope(self):
        """提供事务范围的会话上下文管理器"""
        session = self.ScopedSession()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            self.ScopedSession.remove()
    
    def get_session(self):
        """获取新的会话"""
        return self.ScopedSession()