import os
import sys
p = 'TradeAssistant'                                                        # 项目名
root_path = os.path.join(os.path.abspath(__file__).split(p)[0], p)          # 根路径
if root_path not in sys.path:
    sys.path.append(root_path)
# 数据库配置
DATABASE_BSAE = "data_ro:qwer1234!%40#$@117.50.174.131:10023/marketdata"
# DATABASE_BSAE = "data_ro:qwer1234!%40#$@localhost:5432/marketdata"

DB_URI="mysql+pymysql://ta_admin:StrongPassword123!@127.0.0.1:3306/ta_admin?charset=utf8mb4"
REDIS_URL="redis://localhost:6379/0"

DATABASE_URL = f"postgresql://{DATABASE_BSAE}"
# 日志配置
# INFO\DEBUG\WARNING\ERROR
LOGGING_CONFIG = {
    "LogLevel": "DEBUG",
    "FileName": "ta",
    "FileSize": 25,
    "LogFileDir": "logs"
}


TRADING_SESSION_RULES = {
    # 上海期货交易所金属夜盘
    'cu': [('21:06', '00:55'), ('09:06', '11:30'), ('13:30', '14:55')],
    'al': [('21:06', '00:55'), ('09:06', '11:30'), ('13:30', '14:55')],
    'zn': [('21:06', '00:55'), ('09:06', '11:30'), ('13:30', '14:55')],
    'ni': [('21:06', '00:55'), ('09:06', '11:30'), ('13:30', '14:55')],

    # 股指期货：无夜盘
    'if': [('09:36', '11:30'), ('13:00', '14:55')],
    'ih': [('09:36', '11:30'), ('13:00', '14:55')],
    'ic': [('09:36', '11:30'), ('13:00', '14:55')],
    'im': [('09:36', '11:30'), ('13:00', '14:55')],

    # 铁矿石
    'i': [('21:06', '22:55'), ('09:06', '11:30'), ('13:30', '14:55')],

    # 黄金（无夜盘或夜盘延迟开盘）
    'au': [('21:06', '02:26'), ('09:06', '11:30'), ('13:30', '14:55')],
}


TRADING_Open_SESSION_RULES = {
    # 上海期货交易所金属夜盘
    'cu': [('09:02', '09:30')],
    'al': [('09:02', '09:30')],
    'zn': [('09:02', '09:30')],
    'ni': [('09:02', '09:30')],
    # 股指期货：无夜盘
    'if': [('09:32', '10:00')],
    'ih': [('09:32', '10:00')],
    'ic': [('09:32', '10:00')],
    'im': [('09:32', '10:00')],
    # 铁矿石
    'i':  [('09:02', '09:30')],
    'au': [('09:02', '09:30')],
}
