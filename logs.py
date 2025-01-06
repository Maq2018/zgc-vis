import logging
from logging.handlers import RotatingFileHandler


def configure_log(config):
    file = RotatingFileHandler(config.LOG_PATH, maxBytes=10*1024*1024, backupCount=1)
    # formatter = logging.Formatter(config.LOG_FORMAT, datefmt=config.LOG_DATE_FORMAT)
    # file.setFormatter(formatter)
    # logging.getLogger().addHandler(file)
    logging.basicConfig(level=config.LOG_LEVEL, format=config.LOG_FORMAT, datefmt=config.LOG_DATE_FORMAT, handlers=[file])
