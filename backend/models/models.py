from sqlalchemy import Column, Numeric, String, BigInteger, Integer, Date
from sqlalchemy.dialects.postgresql import TIMESTAMP, JSONB, DATE
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

##############################################future######################################################
class ChinaFuturesDayBar(Base):
    __tablename__ = 'china_futures_daybar'
    instrument_id = Column(String(32), primary_key=True)  # 合约代码
    trading_day = Column(TIMESTAMP(timezone=True), primary_key=True)  # 行情日期
    
    # 价格字段(18位精度,3位小数)
    high_limited = Column(Numeric(18,3))
    low_limited = Column(Numeric(18,3))
    pre_close = Column(Numeric(18,3))
    pre_settlement = Column(Numeric(18,3))
    settlement = Column(Numeric(18,3))
    open_price = Column(Numeric(18,3))
    high_price = Column(Numeric(18,3))
    low_price = Column(Numeric(18,3))
    close_price = Column(Numeric(18,3))
    
    # 量额字段
    volume = Column(BigInteger)  # 成交量
    turnover = Column(Numeric(20,2))  # 成交额
    open_interest = Column(BigInteger)  # 持仓量
    
    # 排名字段
    rank_volume = Column(Integer)  # 成交量排名
    rank_open_interest = Column(Integer)  # 持仓量排名

class ChinaTradingDay(Base):
    __tablename__ = 'china_trading_calendar'
    
    trading_day = Column(Date, nullable=False, primary_key=True)
    pre_trading_day = Column(Date, nullable=False)
    next_trading_day = Column(Date, nullable=False)
    __table_args__ = {'extend_existing': True}  # 允许表已存在时扩展



class ChinaFuturesL1TABar(Base):
    __tablename__ = 'china_futl1_ta_bar1'

    instrument_id = Column(String(32), primary_key=True, nullable=False)  # 合约代码
    trading_day = Column(DATE, primary_key=True, nullable=False)          # 交易日
    time = Column(TIMESTAMP(timezone=True), primary_key=True)             # 时间戳（精确到毫秒）

    open_price = Column(Numeric(18, 3))
    high_price = Column(Numeric(18, 3))
    low_price = Column(Numeric(18, 3))
    close_price = Column(Numeric(18, 3))

    volume = Column(BigInteger)                                           # 成交量（int8）
    turnover = Column(Numeric(20, 2))                                     # 成交额

    uplimit_count = Column(Integer)                                       # 涨停次数
    downlimit_count = Column(Integer)                                     # 跌停次数
    total_cnt = Column(Integer)                                           # 总记录数或交易次数

    bid_price0 = Column(Numeric(18, 3))                                   # 买一价
    bid_vol0 = Column(BigInteger)                                         # 买一量
    ask_price0 = Column(Numeric(18, 3))                                   # 卖一价
    ask_vol0 = Column(BigInteger)                                         # 卖一量



class ChinaFuturesBaseInfo(Base):
    __tablename__ = 'china_futures_base_info'

    instrument_id = Column(String(32), primary_key=True, nullable=False)  # 合约代码
    product = Column(String(32), nullable=False)                          # 品种代码
    exchange = Column(String(32), nullable=False)                         # 交易所代码（如 SHFE、CFFEX）

    multiplier = Column(Integer)                                          # 合约乘数
    tick_size = Column(Numeric(18, 3))                                    # 最小变动价位

    start_trade_date = Column(Date)                                       # 上市日期
    last_trade_date = Column(Date)                                        # 最后交易日
    last_delivery_date = Column(Date)                                     # 最后交割日

    trading_time = Column(JSONB)                                          # 交易时间（JSON结构）


class VChinaFuturesDaybar(Base):
    __tablename__ = 'vchina_futures_daybar'
    __table_args__ = {'extend_existing': True}  # 避免多次声明冲突

    exchange = Column(String(32))                       # 交易所代码，如 SHFE、CFFEX
    product = Column(String(32))                        # 品种代码，如 IF、RB

    trading_day = Column(Date, primary_key=True)        # 行情日期
    instrument_id = Column(String(32), primary_key=True)  # 合约代码

    high_limited = Column(Numeric(18, 3))               # 涨停价
    low_limited = Column(Numeric(18, 3))                # 跌停价
    rank_volume = Column(Integer)                       # 成交量排名
    rank_open_interest = Column(Integer)                # 持仓量排名