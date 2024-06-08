import gc
import json
import os
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen, build_opener, ProxyHandler, OpenerDirector
from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPResponse
import random
from typing import Callable

import numpy as np
import pandas as pd
import requests
from fake_useragent import UserAgent

from api import BantProxy
from config import Config
from utils import measure_time, UtcDate, measure_mem, int2float

_addresses = [
    '3G98jSULfhrES1J9HKfZdDjXx1sTNvHkhN'
    '35pgGeez3ou6ofrpjt8T7bvC9t6RrUK4p6',
    '3HfD4pvF43jdu9dzVMEr1b8AnDHooRGc5t',
    '3FM9vDYsN2iuMPKWjAcqgyahdwdrUxhbJ3',
    '16Uz37Q6TfvK2UFTgamn3dyRjnP2Wtf4p2',
    '157K8VL9g2eLyYrg7nHSWTyjH2KeQ6iD7X',
    '12QVsfAFv5RsHuqx4i9WsNGJPeiYLoN2qo',
    '1DW58ZCSSzpHYn8A7Bpw5g2g6EhunKbdTL',
    '1LyaVPFJitWQEqKBWvwRXV834WLApSz1X5',
    '17RP5JDiYLj1nj7QRvvf4VrAR3Bx1kK15E',
    '3PRJuF2GjvTxqGQngadT1RK5XeHvikNMdQ',
    '15MdAHnkxt9TMC2Rj595hsg8Hnv693pPBB',
    '3GPAWK5aUB5Ve9akvTzZgp69USjgbhFbay',
    '16bhuxop3ZXzA8W3VfUx41gMNGnCbt5ZAB',
    '17sybmVbrihr7GwjZ7bBaKsECnaq9GqPvk',
    '35pbywynSmNrBh2Ps7UESWUCsezmoGVQNH',
    '38fJPq4dYGPoJizEUGCL9yWkqg73cJmC2n',
]

_socks_proxies = [
    'BTkcLymRL:4N7ZEKzim@84.246.82.113:64033',
    '2G3gCniJ2:swq4De455@2.59.212.53:62731',
    'HBWkxRqex:4yUeMJu7g@31.41.251.94:62797',
    'ZGgBYQhqn:R7bcaiBV1@176.126.102.164:63127',
    '7fc679DaK:i94xNvnRW@176.126.115.229:64973',
    'bVPGANPha:kyvYkrqKA@176.116.23.217:63451',
    'wErWgweRd:d3Nag4GER@46.3.94.145:64335',
    'mQ1dCUJyD:HRScMauzp@176.116.15.32:64453',
    'H2umfhcPn:gzzzgGBLC@176.116.2.8:62931',
    'AwczwHgfx:nqwk7G9VX@176.116.20.173:64165',
]


_http_proxies = [
    'AwczwHgfx:nqwk7G9VX@176.116.20.173:64164',
    'wErWgweRd:d3Nag4GER@46.3.94.145:64334',
    'BTkcLymRL:4N7ZEKzim@84.246.82.113:64032',
    'mQ1dCUJyD:HRScMauzp@176.116.15.32:64452',
    'H2umfhcPn:gzzzgGBLC@176.116.2.8:62930',
    'HBWkxRqex:4yUeMJu7g@31.41.251.94:62796',
    'ZGgBYQhqn:R7bcaiBV1@176.126.102.164:63126',
    '2G3gCniJ2:swq4De455@2.59.212.53:62730',
    '7fc679DaK:i94xNvnRW@176.126.115.229:64972',
    'bVPGANPha:kyvYkrqKA@176.116.23.217:63450',
]

socks_proxies: list[BantProxy] = [BantProxy(proxy, 10) for proxy in Config().socks_proxies]
http_proxies: list[BantProxy] = [BantProxy(proxy, 10) for proxy in Config().http_proxies]


def csv2df(subdir='') -> dict[str, pd.DataFrame]:
    def _csv2df(dfname) -> pd.DataFrame | None:
        fn_csv = f'csv/{dfname}.csv' if subdir == '' else f'csv/{subdir}/{dfname}.csv'
        if not os.path.exists(fn_csv):
            return None
        _df = pd.read_csv(fn_csv, sep=';')
        if 'label' in _df.columns:
            _df['label'] = _df['label'].fillna('')
        if 'ts_finish' in _df.columns:
            _df['ts_finish'] = _df['ts_finish'].replace(np.nan, None)
        return _df

    df_info = {
        'top_slice': _csv2df('top_slice'),
        'last_slice': _csv2df('last_slice'),
        'non_changed': _csv2df('non_changed'),
        'new_slice': _csv2df('new_slice'),
        'exists_slice': _csv2df('exists_slice'),
        'upd_slice': _csv2df('upd_slice'),
        'upd_tiny_slice': _csv2df('upd_tiny_slice')
    }
    return df_info


def calc_limit(upd_line) -> int:
    n_tr = upd_line.n_tr
    if n_tr < 50:
        return 100
    if n_tr <= 1000:
        n_tr = n_tr * 2
        return n_tr
    if 1000 < n_tr <= 2800:
        return n_tr
    else:
        n_tr = n_tr - upd_line.n_tr_prev
        if n_tr < 1000:
            n_tr += 1000
            return n_tr
    return 2800


def get_user_agent():
    ua = UserAgent()
    useragent = ua.random
    return useragent


def get_socks_proxy():
    if socks_proxies is None or len(socks_proxies) == 0:
        return None
    pr: list[BantProxy] = [p for p in socks_proxies if p.is_awailable()]
    while len(pr) == 0:
        time.sleep(0.5)
        pr: list[BantProxy] = [p for p in socks_proxies if p.is_awailable()]
    pr.sort(key=lambda p: p.count)
    ok, http_proxy = pr[0].get_poxy()
    proxy = {'https': f'socks5://{http_proxy}'}
    # proxy = http_proxy
    return proxy


def get_http_proxy():
    if socks_proxies is None or len(socks_proxies) == 0:
        return None
    pr: list[BantProxy] = [p for p in http_proxies if p.is_awailable()]
    while len(pr) == 0:
        time.sleep(0.5)
        pr: list[BantProxy] = [p for p in socks_proxies if p.is_awailable()]
    pr.sort(key=lambda p: p.count)
    ok, http_proxy = pr[0].get_poxy()
    proxy = {'https': f'http://{http_proxy}'}
    # proxy = http_proxy
    return proxy


def rawaddr_to_txs(data) -> [dict]:
    """Результат запроса rawaddr в список транзакций"""
    def _to_float(item):
        """Преобразовать из int в float"""
        keys = ['tx_btc', 'balance_btc']
        for key in keys:
            item[key] = int2float(item[key])
        return item

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
    txs = list(map(lambda item: _to_float(item), txs))
    return txs


def requests_get_transactions(address, limit=200, offset=0, proxy=None) -> tuple[pd.DataFrame | None, str]:
    """Запросить и вернуть транзакции по заданному адресу кошелька"""
    base_url = 'https://blockchain.info'
    url = f'{base_url}/rawaddr/{address}/?limit={limit}&offset={offset}'
    useragent = get_user_agent()
    headers = {'useragent': useragent}

    # делаем апи запрос
    try:
        with requests.get(url, headers=headers, proxies=proxy, stream=True) as r:
        # with requests.Session() as session:
        #     r = session.get(url, headers=headers, proxies=proxy, stream=True)
            status_code = r.status_code
            if status_code == 200:
                data = r.json()
                txs = rawaddr_to_txs(data)
                df: pd.DataFrame = pd.DataFrame(txs)
                return df, ''
            if status_code == 429:
                error = f'status_code={status_code}. Too Many Requests.  RateLimitError with proxies = {proxy}'
            elif status_code == 524:
                error = f'status_code={status_code}. The web server timed out before responding. With proxies = {proxy}'
            else:
                error = f'status_code={status_code}. {r.text}. With proxies = {proxy}'
    except Exception as ex:
        error = f'Неизвестная ошибка = {ex}'
    return None, error


def urllib_get_transactions(address, limit=2000, offset=0, proxy=None) -> tuple[pd.DataFrame | None, str]:
    def open_url(func: Callable[[Request], HTTPResponse], request: Request):
        _status_code = -1
        with func(request) as _response:
            _status_code = _response.status
            _body = _response.read()
            _decoded_body = _body.decode("utf-8")
        _data = json.loads(_decoded_body) if _status_code == 200 else _decoded_body
        return _status_code, _data

    base_url = 'https://blockchain.info'
    _url = f'{base_url}/rawaddr/{address}/?limit={limit}&offset={offset}'
    useragent = get_user_agent()
    headers = {'useragent': useragent}
    _request = Request(_url, headers=headers)

    _func = urlopen
    if proxy:
        opener = build_opener(ProxyHandler({'http': f'socks5://{proxy}'}))
        _func = opener.open

    df = None
    try:
        status_code, data = open_url(_func, _request)
        # print(f'"{address}" {status_code=}, {len(data)=}; "{proxy=}"')
        match status_code:
            case 200:
                txs = rawaddr_to_txs(data)
                df: pd.DataFrame = pd.DataFrame(txs)
                error = ''
            case 524:
                error = f'status_code={status_code}. The web server timed out before responding. With proxies = {proxy}'
            case _:
                error = f'status_code={status_code}. {data}. With proxies = {proxy}'
    except HTTPError as ex:
        error = ex
    except Exception as ex:
        error = f'Неизвестная ошибка = {ex}'
    return df, error


# @profile_mem
def update_address(upd_line, cat='socks') -> tuple[pd.DataFrame | None, str]:
    """
    Выполнить обновление кошелька
    :param upd_line: строка последнего среза кошелька
    :param cat: socks | http | None
    :return: успех|не успех, строка с ошибкой
    """
    # вызовем api для получения спска транзакций
    address = upd_line.address
    limit = calc_limit(upd_line)
    match cat:
        case 'socks':
            proxy = get_socks_proxy()
        case 'http':
            proxy = get_http_proxy()
        case _:
            proxy = None

    tr_df, _error = requests_get_transactions(address, limit, proxy=proxy)
    # tr_df, _error = urllib_get_transactions(address, limit, proxy=proxy)
    return tr_df, _error


def run_upd_thread_pool_executor(upd_slice: pd.DataFrame, max_workers: int, cat: str):
    """Загрузить транзакции, преобразовать их в срезы"""
    futures = []
    executor = ThreadPoolExecutor(max_workers=max_workers)
    done_count = 0
    start_count = 0
    for idx, upd_line in upd_slice.iterrows():
        # запусить все обработчики
        # future = executor.submit(update_address, upd_line, 'socks')
        future = executor.submit(update_address, upd_line, cat)
        futures.append(future)
        start_count = start_count + 1
        if len(futures) < max_workers:
            continue

        # проверим, есть ли завершенные задачи
        done = []
        print(f'{start_count=}; {done_count=}')
        while len(done) == 0:
            done = [future for future in futures if future.done()]
            if len(done) == 0:
                time.sleep(1.0)

        # обработка завершенных задач / получение результата
        for future in done:
            res, error = future.result()
            print(f'tr_df={len(res) if isinstance(res, pd.DataFrame) else None}, "{error=}"')
            futures.remove(future)
            done_count = done_count + 1

    # проверим, есть ли завершенные задачи
    while len(futures) != 0:
        done = [future for future in futures if future.done()]
        if len(done) == 0:
            time.sleep(1.0)
        else:
            for future in done:
                futures.remove(future)
                done_count = done_count + 1
                print(f'{done_count=}')

    print('wait shutdowt thread pool executor')
    executor.shutdown(wait=True)
    print('-' * 100)
    print(len(upd_slice), start_count, done_count)


@measure_time
@measure_mem
def requests_with_socks_proxy(proxy_id):
    address = '35pgGeez3ou6ofrpjt8T7bvC9t6RrUK4p6'
    base_url = 'https://blockchain.info'
    limit = 50
    offset = 0
    url = f'{base_url}/rawaddr/{address}/?limit={limit}&offset={offset}'

    useragent = get_user_agent()
    headers = {'useragent': useragent}
    request = Request(url, headers=headers)

    proxy = _socks_proxies[proxy_id]
    proxy = {'socks': f'socks5://{proxy}'}

    with requests.Session() as session:
        r = session.get(url, headers=headers, proxies=proxy, stream=True)
        status_code = r.status_code
        print(f'{status_code=}')

    print(f'{url=}; {proxy=}')


@measure_time
@measure_mem
def urlib_with_proxy(proxy_id):
    address = '35pgGeez3ou6ofrpjt8T7bvC9t6RrUK4p6'
    base_url = 'https://blockchain.info'
    limit = 50
    offset = 0
    url = f'{base_url}/rawaddr/{address}/?limit={limit}&offset={offset}'

    useragent = get_user_agent()
    headers = {'useragent': useragent}
    request = Request(url, headers=headers)

    proxy = _socks_proxies[proxy_id]
    opener: OpenerDirector = build_opener(ProxyHandler({'socks': f'socks5://{proxy}'}))

    with opener.open(request) as r:
        status_code = r.status
        print(f'{status_code=}')

    print(f'{url=}; {proxy=}')


@measure_time
@measure_mem
def try_requests_with_http_proxy():
    """Пробую requests с http прокси"""
    count_before = len(gc.get_objects())

    proxy_str = random.choice(_http_proxies)
    proxy = {'https': f'http://{proxy_str}'}
    address = random.choice(_addresses)

    tr_df, err = requests_get_transactions(address, proxy=proxy)
    print(f'{address}; tr_df={len(tr_df) if tr_df is not None else tr_df}, error="{err}", proxy={proxy_str}')
    count_after_1 = len(gc.get_objects())
    gc.collect(0)
    gc.collect(1)
    gc.collect(2)

    count_after_2 = len(gc.get_objects())
    delta = (count_after_2 - count_before)
    print(f'{count_before=};  {count_after_1=};  {count_after_2=}; {delta=}')


@measure_time
@measure_mem
def run_thread_pool(max_workers: int, cat: str):
    """"""
    di_5 = csv2df('step_5')
    # di_5 = {k: v.set_index('address').sort_index() for k, v in di_5.items() if v is not None}
    upd_slice = di_5['upd_slice']
    assert len(upd_slice) == 405
    run_upd_thread_pool_executor(upd_slice, max_workers, cat=cat)
    # run_upd_thread_pool_executor(upd_slice[0:], max_count)


if __name__ == '__main__':
    run_thread_pool(4, 'socks')     # запусить обработку с socks прокси
    run_thread_pool(4, 'http')      # запусить обработку с http прокси
    # requests_with_socks_proxy(proxy_id=0)
    # urlib_with_proxy(proxy_id=8)
    # try_requests_with_http_proxy()
    pass
