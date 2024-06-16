import gc
import json
import os
import random
import tempfile
import time

import pandas as pd
import requests
from fake_useragent import UserAgent
from requests.exceptions import ChunkedEncodingError, RequestException, HTTPError
from urllib3.exceptions import ConnectTimeoutError

from config import Config
from utils import satoshi2btc, UtcDate


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

        self.with_proxy = False                   # признак использования прокси запросе данных через инет

    def get_addr_balances(self, addresses) -> tuple[pd.DataFrame | None, str]:
        """Вернуть текущие балансы для списка кошельков"""
        addresses = '|'.join(addresses)
        url = f'{self.base_url}/balance?active={addresses}'
        useragent = self.get_user_agent()
        headers = {'useragent': useragent}

        # делаем апи запрос
        error = ''
        try:
            with requests.get(url, headers=headers) as r:
                r.raise_for_status()
                if r.ok:
                    # обработать результат
                    _data = r.json()
                    data = []
                    for k, v in _data.items():
                        v['address'] = k
                        data.append(v)
                    _df: pd.DataFrame = pd.DataFrame(data)
                    _df['final_balance'] = _df['final_balance'].apply(lambda x: satoshi2btc(x))
                    return _df[['address', 'n_tx', 'final_balance']], error
        except HTTPError as ex:
            error = f'HTTPError; error = "{ex}"'
        # except ChunkedEncodingError as ex:
        #     error = f'ChunkedEncodingError error = "{ex}"'
        except RequestException as ex:
            error = f'RequestException; error= "{ex}"'
        except ConnectTimeoutError:
            error = f'ConnectTimeoutError'
        except Exception as ex:
            error = f'Неизвестная ошибка; type={type(ex)}; error="{ex}"'
        finally:
            gc.collect()
        return None, error

    def get_transactions(self, address, limit=200, offset=0, proxy=None) -> tuple[pd.DataFrame | None, str]:
        """Запросить и вернуть транзакции по заданному адресу кошелька"""
        url = f'{self.base_url}/rawaddr/{address}/?limit={limit}&offset={offset}'
        useragent = self.get_user_agent()
        headers = {'useragent': useragent}

        # делаем апи запрос
        error = ''
        try:
            with requests.get(url, headers=headers, proxies=proxy, stream=True) as r:
                r.raise_for_status()
                if r.ok:
                    data = r.json()
                    txs = self.rawaddr_to_txs(data)
                    tr_df: pd.DataFrame = pd.DataFrame(txs)
                    return tr_df, error
        except HTTPError as ex:
            error = f'HTTPError with porxy: {proxy}; error = "{ex}"'
        except ChunkedEncodingError as ex:
            error = f'ChunkedEncodingError with proxy = {proxy} error = "{ex}"'
        except RequestException as ex:
            error = f'RequestException with proxy: {proxy}; error= "{ex}"'
        except ConnectTimeoutError:
            error = f'ConnectTimeoutError with proxy = {proxy}'
        except Exception as ex:
            error = f'Неизвестная ошибка witha porxy: {proxy};  type={type(ex)}; error="{ex}"'
        finally:
            gc.collect()
        return None, error

    def requests_chunk_get_transactions(self, address, limit=200, offset=0, proxy=None,
                                        chunk_size: int = 10240) -> tuple[pd.DataFrame | None, str]:
        """Запросить и вернуть транзакции по заданному адресу кошелька"""
        url = f'{self.base_url}/rawaddr/{address}/?limit={limit}&offset={offset}'
        useragent = self.get_user_agent()
        headers = {'useragent': useragent}

        # делаем апи запрос
        error = ''
        try:
            with requests.get(url, headers=headers, proxies=proxy, stream=True) as r:
                r.raise_for_status()
                if r.ok:
                    temp_file_name = tempfile.mktemp()
                    with open(temp_file_name, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=chunk_size):
                            f.write(chunk)

                    # прочитать результат
                    with open(temp_file_name, 'rb') as f:
                        txt = f.read()
                    # удалить временный файл
                    os.remove(temp_file_name)

                    # обработать результат
                    data = json.loads(txt)
                    txs = self.rawaddr_to_txs(data)
                    tr_df: pd.DataFrame = pd.DataFrame(txs)
                    return tr_df, error
        except HTTPError as ex:
            error = f'HTTPError with porxy: {proxy}; error = "{ex}"'
        except ChunkedEncodingError as ex:
            error = f'ChunkedEncodingError with proxy = {proxy} error = "{ex}"'
        except RequestException as ex:
            error = f'RequestException with proxy: {proxy}; error= "{ex}"'
        except Exception as ex:
            error = f'Неизвестная ошибка witha porxy: {proxy};  type={type(ex)}; error="{ex}"'
        finally:
            gc.collect()
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
        return txs


class BinanceApi:
    def __init__(self):
        pass

    def get_prices(self, all_ts: list[int], is_binance_ts: bool) -> list[float | None]:
        if not is_binance_ts:
            all_ts = [UtcDate.ts2binance_ts(ts) for ts in all_ts]

        results = []
        for ts in all_ts:
            price = random.choice([35800.0, 34900.0, 37700.0, 39389.05, 42700.0, 43100.0, 55900.0])
            results.append(price)
        return results
