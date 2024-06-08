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

        # proxies socks and http
        self.socks_proxies: list[str] = self._load_proxies_('socks')
        self.http_proxies: list[str] = self._load_proxies_('http')
        # redis
        chunk_size_str: str = os.getenv('CHUNK_SIZE', 20480)
        self.chunk_size: int = int(chunk_size_str)

    def _load_proxies_(self, cat='socks') -> list[str]:
        """Загрузисть socks или http прокси из файла"""
        _path = f'{self.app_dir}/{cat}_proxies.txt'
        if not os.path.exists(_path):
            return []
        with open(_path, mode='rt', encoding='utf-8') as f:
            lines = f.readlines()
        lst = [line[0:-1] for line in lines if not line.startswith('#')]
        return lst
