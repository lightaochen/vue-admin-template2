# utils/logger.py
import os
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from datetime import datetime
import logging
p = 'TradeAssistant'
root_path = os.path.join(os.path.abspath(__file__).split(p)[0], p)
if root_path not in sys.path:
    sys.path.append(root_path)
from config.settings import LOGGING_CONFIG

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

class MyLogger:
    def __init__(self, name: str, logDir: str = "logs", level: str = "INFO", maxSize:int = 100, coreId:int = -1):
        if coreId > 0:
            os.sched_setaffinity(0, [coreId])
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        
        Path(logDir).mkdir(parents=True, exist_ok=True)
        
        fileHandler = RotatingFileHandler(
            filename=f"{logDir}/{name}.log",
            maxBytes=maxSize*1024*1024,
            backupCount=3,
            encoding='utf-8',
            mode='a'
        )
        

        fileHandler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
        
        consoleHandler = logging.StreamHandler(sys.stdout)
        consoleHandler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))

        self.logger.addHandler(fileHandler)
        self.logger.addHandler(consoleHandler)
        
        self.logger.propagate = False

    def getLogger(self):
        return self.logger

logLevel = LOGGING_CONFIG["LogLevel"]
logFileName = LOGGING_CONFIG["FileName"]
fileSize = int(LOGGING_CONFIG["FileSize"])
logDir = LOGGING_CONFIG["LogFileDir"]
currentTime = datetime.now().strftime("%Y-%m-%d")
coreId = -1
if "CoreId" in LOGGING_CONFIG:
    numCores = os.cpu_count()
    tCoreId = int(LOGGING_CONFIG["CoreId"])
    if tCoreId > 0 and tCoreId < numCores:
        coreId = tCoreId
systemLogger = MyLogger(f"{logFileName}_{currentTime}", logDir, logLevel, fileSize, coreId).getLogger()

def handleException(exc_type, exc_value, exc_traceback):
    systemLogger.error(
        "Uncaught exception", 
        exc_info=(exc_type, exc_value, exc_traceback)
    )

sys.excepthook = handleException
