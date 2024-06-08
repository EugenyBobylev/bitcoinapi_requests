import os
from pathlib import Path

from dotenv import load_dotenv


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class Config(metaclass=SingletonMeta):
    def __init__(self):
        script_path = os.path.abspath(__file__)
        env_path = Path(script_path).parent / '.env'
        load_dotenv(env_path)

        self.app_dir: str = str(Path(script_path).parent)
        self.logs_dir = f"{self.app_dir}/logs"

        # proxies socks and http
        self.socks_proxies: list[str] = self._load_proxies_('socks')
        self.http_proxies: list[str] = self._load_proxies_('http')
        # redis
        _port = os.getenv('REDIS_PORT', None)
        self.redis_port: int = int(_port) if _port else None
        self.redis_password: str = os.getenv('REDIS_PASSWORD', None)
        # cron
        self.minutes_cron_update_market_data: str = os.getenv('MINUTES_CRON_UPDATE_MARKET_DATE')
        self.hours_cron_update_market_data: str = os.getenv('HOURS_CRON_UPDATE_MARKET_DATE')

    def _load_proxies_(self, cat='socks') -> list[str]:
        """Загрузисть socks или http прокси из файла"""
        _path = f'{self.app_dir}/{cat}_proxies.txt'
        if not os.path.exists(_path):
            return []
        with open(_path, mode='rt', encoding='utf-8') as f:
            lines = f.readlines()
        lst = [line[0:-1] for line in lines if not line.startswith('#')]
        return lst
