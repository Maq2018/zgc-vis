import logging
from typing import Dict, Any
from pydantic_settings import BaseSettings


logger = logging.getLogger("config")


class DevSettings(BaseSettings):
    MODE:str = 'DEV'
    MONGO_MAP: Dict[str, Any] = dict(
        default=dict(
            DATABASE="db",
            READ_PREFERENCE="SECONDARY_PREFERRED",
            MAX_POOL_SIZE=20,
            HOSTS=['localhost:27017'],
            ASYNC=True,
        ),
        default_sync=dict(
            DATABASE="db",
            READ_PREFERENCE="SECONDARY_PREFERRED",
            MAX_POOL_SIZE=20,
            HOSTS=['localhost:27017'],
            ASYNC=False,
        ),
    )
    LOG_PATH: str = "./log.txt"
    LOG_LEVEL: int = logging.DEBUG
    LOG_FORMAT: str = "[%(asctime)s - %(name)s - %(lineno)d] %(levelname)s: %(message)s"
    LOG_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"


def get_config():
    return DevSettings()


Config = get_config()
