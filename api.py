import copy
import time

import pandas as pd
import requests

from fake_useragent import UserAgent
from requests.exceptions import ChunkedEncodingError
from urllib3.exceptions import ConnectTimeoutError

from config import Config
from utils import UtcDate


class BantProxy:
    __slots__ = ('_proxy', '_delay', '_count', '_is_active', '_ts')

    def __init__(self, proxy: str, delay: int = 10):
        self._proxy: str = proxy  # сам poxy
        self._delay: int = delay  # задеркка в сек между выдачей прокси
        self._count: int = 0  # счетчик количества использований прокси
        self._is_active: bool = True  # признак доступности прокси для использования
        self._ts = UtcDate.now_ts() - delay - 2  # метека времени последней выдачи прокси

    def __str__(self):
        return f'proxy={self._proxy}; delay={self._delay}; count={self._count}; is_active={self.is_active}'

    @property
    def proxy(self) -> str:
        return self._proxy

    @property
    def delay(self) -> int:
        return self._delay

    @delay.setter
    def delay(self, value):
        if not isinstance(value, int) or value < 0:
            raise ValueError('Delay must be a positive int`')
        if value != self._delay:
            self._delay = value

    @property
    def count(self):
        return self._count

    @property
    def is_active(self):
        return self._is_active

    @is_active.setter
    def is_active(self, value):
        if not isinstance(value, bool):
            raise ValueError("is_active must be bool")
        if value is not self._is_active:
            self._is_active = value

    def is_awailable(self) -> bool:
        """Доступность прокси для использования"""
        return self.is_active and UtcDate.now_ts() > (self._ts + self._delay)

    def reset_count(self):
        """Сброс счетчика использвания прокси"""
        self._count = 0

    def get_poxy(self) -> tuple[bool, str]:
        """Выдать прокси для использования"""
        ok = self.is_awailable()
        if not ok:
            return False, ''
        self._count += 1
        self._ts = UtcDate.now_ts()
        return True, self._proxy


class BitcoinApi:
    def __init__(self):
        self.base_url = 'https://blockchain.info'
        self.socks_proxies: list[BantProxy] = [BantProxy(proxy, 10) for proxy in Config().socks_proxies]
        self.http_proxies: list[BantProxy] = [BantProxy(proxy, 10) for proxy in Config().http_proxies]

        self.with_proxy = False                   # признак использования прокси в запросе данных через инет

    def get_transactions(self, address, limit=200, offset=0, use_proxy: str = '') -> tuple[pd.DataFrame | None, str]:
        """Запросить и вернуть транзакции по заданному адресу кошелька"""
        url = f'{self.base_url}/rawaddr/{address}/?limit={limit}&offset={offset}'
        useragent = self.get_user_agent()
        headers = {'useragent': useragent}

        match use_proxy:
            case 'socks':
                proxy_str = self.get_proxy(cat='socks')
                proxy = {'socks': f'socks5://{proxy_str}'}
            case 'http':
                proxy_str = self.get_proxy(cat='http')
                proxy = {'http': f'http://{proxy_str}'}
            case _:
                proxy_str = None
                proxy = None

        # делаем апи запрос
        try:
            # with requests.get(url, headers=headers, proxies=proxy, stream=True) as r:
            with requests.Session() as session:
                r = session.get(url, headers=headers, proxies=proxy, stream=True)
                status_code = r.status_code
                if status_code == 200:
                    data = r.json()
                    txs = self.rawaddr_to_txs(data)
                    df: pd.DataFrame = pd.DataFrame(txs)
                    return df, ''
                if status_code == 429:
                    error = f'status_code={status_code}. Too Many Requests.  With proxies = {proxy_str}'
                elif status_code == 524:
                    error = (f'status_code={status_code}. The web server timed out before responding. '
                             f'With proxies = {proxy_str}')
                else:
                    error = f'status_code={status_code}. {r.text}. With proxies = {proxy_str}'
        except ConnectTimeoutError:
            error = f'ConnectTimeoutError with proxy = {proxy_str}'
        except ChunkedEncodingError as ex:
            error = f'ChunkedEncodingError with proxy = {proxy_str} error = "{ex}"'
        except Exception as ex:
            error = f'Неизвестная ошибка = "{ex}"'
        return None, error

    def get_proxy(self, cat: str):
        """Дать доступный прокси"""
        ok = False
        pr = []
        if cat == 'socks':
            if self.socks_proxies is None or len(self.socks_proxies) == 0:
                return None
            while not ok:
                time.sleep(0.5)
                pr: list[BantProxy] = [p for p in self.socks_proxies if p.is_awailable()]
                ok = len(pr) > 0
        else:
            if self.http_proxies is None or len(self.http_proxies) == 0:
                return None
            while not ok:
                time.sleep(0.5)
                pr: list[BantProxy] = [p for p in self.http_proxies if p.is_awailable()]
                ok = len(pr) > 0
        pr.sort(key=lambda p: p.count)
        _, proxy = pr[0].get_poxy()
        return proxy

    @staticmethod
    def get_user_agent():
        ua = UserAgent()
        useragent = ua.random
        return useragent

    @staticmethod
    def rawaddr_to_txs(data) -> [dict]:
        """Результат запроса rawaddr в список транзакций"""
        if (not isinstance(data, dict)) or ('address' not in data) or ('txs' not in data):
            return []
        address = data['address']
        txs = data['txs']
        txs = [
            {
                'address': address,
                'block': tx['block_index'],
                'time': UtcDate.ts2dt(tx['time']),
                'tx_btc': tx['result'],
                'balance_btc': tx['balance'],
                'tx_index': tx['tx_index'],
                'ts': tx['time'],
            } for tx in txs]
        return copy.deepcopy(txs)
