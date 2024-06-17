import gc
import json
import os
import tempfile
import time
from multiprocessing import Queue
from urllib.error import HTTPError
from concurrent.futures import ThreadPoolExecutor
import random

import numpy as np
import pandas as pd
import requests
from fake_useragent import UserAgent
from requests import RequestException
from requests.exceptions import ChunkedEncodingError

from api import BantProxy, BinanceApi
from config import Config
from utils import measure_time, UtcDate, measure_mem, satoshi2btc, slices_data

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

base_url = 'https://blockchain.info'


cohorts = [
    {'min_val': 0, 'max_val': 1, 'code': '0'},
    {'min_val': 1, 'max_val': 100, 'code': 'a'},
    {'min_val': 100, 'max_val': 1000, 'code': 'b'},
    {'min_val': 1000, 'max_val': 10000, 'code': 'c'},
    {'min_val': 10000, 'max_val': 100000, 'code': 'd'},
    {'min_val': 100000, 'max_val': None, 'code': 'e'},
]


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


def get_cohort(btc: float) -> str:
    """Определить когорту"""
    for cohort_data in cohorts:
        if cohort_data['max_val'] is None:
            return cohort_data['code']
        if cohort_data['min_val'] <= btc < cohort_data['max_val']:
            return cohort_data['code']
    return ''


def calc_limit(upd_line) -> int:
    max_tr = 2800
    n_tr = upd_line.n_tr
    if n_tr < 0:
        return max_tr
    if n_tr < 50:
        return 100
    if n_tr <= 1000:
        n_tr = n_tr * 2
        return n_tr
    if 1000 < n_tr <= max_tr:
        return n_tr
    else:
        n_tr = n_tr - upd_line.n_tr_prev
        if n_tr < 1000:
            n_tr += 1000
            return n_tr
    return max_tr


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
    if socks_proxies is None or len(http_proxies) == 0:
        return None
    pr: list[BantProxy] = [p for p in http_proxies if p.is_awailable()]
    while len(pr) == 0:
        time.sleep(0.5)
        pr: list[BantProxy] = [p for p in http_proxies if p.is_awailable()]
    pr.sort(key=lambda p: p.count)
    ok, http_proxy = pr[0].get_poxy()
    proxy = {'https': f'http://{http_proxy}'}
    return proxy


def rawaddr_to_txs(data) -> [dict]:
    """Результат запроса rawaddr в список транзакций"""
    def _to_float(item):
        """Преобразовать из int в float"""
        keys = ['tx_btc', 'balance_btc']
        for key in keys:
            item[key] = satoshi2btc(item[key])
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
    url = f'{base_url}/rawaddr/{address}/?limit={limit}&offset={offset}'
    useragent = get_user_agent()
    headers = {'useragent': useragent}

    # делаем апи запрос
    try:
        with requests.get(url, headers=headers, proxies=proxy, stream=True) as r:
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


is_remove_tmp = True


def requests_chunk_get_transactions(address, limit=200, offset=0, proxy=None, chunk_size: int = 10240) -> tuple[pd.DataFrame | None, str]:
    """Запросить и вернуть транзакции по заданному адресу кошелька"""
    url = f'{base_url}/rawaddr/{address}/?limit={limit}&offset={offset}'
    useragent = get_user_agent()
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
                txs = rawaddr_to_txs(data)
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


def get_addr_balances(addresses) -> tuple[pd.DataFrame | None, str]:
    """Вернуть текущие балансы для списка кошельков"""
    addresses = '|'.join(addresses)
    url = f'{base_url}/balance?active={addresses}'
    useragent = get_user_agent()
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
    except RequestException as ex:
        error = f'RequestException; error= "{ex}"'
    except Exception as ex:
        error = f'Неизвестная ошибка; type={type(ex)}; error="{ex}"'
    finally:
        gc.collect()
    return None, error


# @profile_mem
def tr2slices(tr_df: pd.DataFrame, df_line: pd.Series, cat_line='update') -> pd.DataFrame | None:
    """
    Получить историяю транзакций, создать недостающие срезы
    :param tr_df: dataframe транзакций в формате bitcoinapi
    :param df_line: инофрмация о последнем имеющимся в БД срезе
    :param cat_line: м.б. update or new
    :return: DataFrame в формате среза slices
    """

    # оставим только те транзакии которых нет в slices
    balance_btc_prev = df_line.balance_btc_prev if cat_line == 'update' else 0.0

    if cat_line == 'update':
        try:
            idx = tr_df.index[tr_df['balance_btc'] == balance_btc_prev][0]
            tr_df: pd.DataFrame = tr_df[tr_df.index < idx]
        except IndexError as ex:         # Если не нашли такой баланс в списке транзакций
            ts_prev = df_line.ts_prev   # тогда будем искать по метке времени
            tr_df: pd.DataFrame = tr_df[tr_df.ts > ts_prev]

    tr_df = tr_df.sort_index(ascending=False, ignore_index=True)
    tr_df['label'] = df_line['label']
    tr_df['first_tr'] = df_line['first_tr']
    tr_df['last_tr'] = tr_df['time'].apply(lambda dt: UtcDate.dt2str(dt))
    tr_df['n_tr'] = df_line['n_tr'] + tr_df.index + 1
    tr_df['ts'] = tr_df['ts'] - (tr_df['ts'] % 3600)
    tr_df['time'] = tr_df['ts'].apply(lambda ts: UtcDate.ts2str(ts))
    tr_df = tr_df.sort_values('ts', ascending=False, ignore_index=True)
    tr_df = tr_df.drop_duplicates(subset=['time'], ignore_index=True)
    tr_df['ts_finish'] = tr_df['ts'].shift(1).replace(np.nan, None)

    all_ts = list(tr_df['ts'])
    binance = BinanceApi()
    prices = binance.get_prices(all_ts, False)

    tr_df['exchange_rate'] = pd.Series(prices)
    tr_df['balance_usd'] = round(tr_df['balance_btc'] * tr_df['exchange_rate'], 8)
    tr_df['cohort'] = tr_df['balance_btc'].apply(lambda btc: get_cohort(btc))

    # prof = prev_balance_btc * (price - prev_price)
    tr_df['tr_profit'] = round(tr_df['balance_btc'].shift(-1) * (tr_df['exchange_rate'] - tr_df['exchange_rate'].shift(-1)), 8)

    # calc tr_profit для самой первой транзакции в обновлении, она находится в конце dataframe
    tr_profit = 0.0
    if cat_line == 'update':
        last = tr_df.iloc[-1]
        tr_profit = round(df_line['balance_btc'] * (last['exchange_rate'] - df_line['exchange_rate']), 8)
    tr_df.loc[tr_df.index[-1], 'tr_profit'] = tr_profit

    tr_df = tr_df.sort_index(ascending=False, ignore_index=True)
    tr_df['cum_profit'] = tr_df['tr_profit'].cumsum()
    tr_df['cum_profit'] = tr_df['cum_profit'] + df_line['cum_profit']
    tr_df['changed'] = False

    df = tr_df.rename(columns={'tx_btc': 'tr_btc', 'block': 'last_block'}, )
    df = df[['address', 'label', 'tr_btc', 'balance_btc', 'balance_usd', 'first_tr', 'last_tr', 'n_tr', 'time',
             'ts', 'ts_finish', 'exchange_rate', 'tr_profit', 'cum_profit', 'cohort', 'last_block', 'changed']]
    return df


def update_address(df_line, cat_line='update', cat='socks') -> tuple[pd.DataFrame | None, str]:
    """
    Выполнить обновление кошелька
    :param df_line: строка последнего среза кошелька
    :param cat_line: 'update' or 'new', default update
    :param cat: socks | http | None
    :return: успех|не успех, строка с ошибкой
    """
    # вызовем api для получения спска транзакций
    address = df_line.address
    limit = calc_limit(df_line)
    match cat:
        case 'socks':
            proxy = get_socks_proxy()
        case 'http':
            proxy = get_http_proxy()
        case _:
            proxy = None

    tr_df, _error = requests_chunk_get_transactions(address, limit, proxy=proxy)
    # tr_df, _error = requests_get_transactions(address, limit, proxy=proxy)
    # tr_df, _error = urllib_get_transactions(address, limit, proxy=proxy)

    slice_df = None
    if _error != '':
        print(_error)
        return tr_df, _error
    slice_df = tr2slices(tr_df, df_line, cat_line)
    return slice_df, ''


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
            print(f'tr_df={len(res) if isinstance(res, pd.DataFrame) else None}, {error=}')
            futures.remove(future)
            done_count = done_count + 1

    # проверим, есть ли завершенные задачи
    while len(futures) != 0:
        done = [future for future in futures if future.done()]
        if len(done) == 0:
            time.sleep(1.0)
        else:
            for future in done:
                res, error = future.result()
                print(f'tr_df={len(res) if isinstance(res, pd.DataFrame) else None}, {error=}')
                futures.remove(future)
                done_count = done_count + 1
                print(f'{done_count=}')

    print('wait shutdowt thread pool executor')
    executor.shutdown(wait=True)
    print('-' * 100)
    print(len(upd_slice), start_count, done_count)


@measure_time
@measure_mem
def requests_with_proxy(idx):
    address = '35pgGeez3ou6ofrpjt8T7bvC9t6RrUK4p6'
    limit = 50
    offset = 0
    url = f'{base_url}/rawaddr/{address}/?limit={limit}&offset={offset}'

    useragent = get_user_agent()
    headers = {'useragent': useragent}

    proxy = _socks_proxies[idx]
    proxy = {'https': f'socks5://{proxy}'}

    with requests.Session() as session:
        r = session.get(url, headers=headers, proxies=proxy, stream=True)
        status_code = r.status_code
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
    # run_upd_thread_pool_executor(upd_slice[:50], max_workers, cat=cat)


@measure_time
@measure_mem
def update_new_address():
    """Найдем все транзакции для одного кошелька"""
    di_5 = csv2df('step_5')
    new_slice: pd.DataFrame = di_5['new_slice']
    new_line = new_slice.loc[1414]

    slice_df, error = update_address(new_line, cat='socks', cat_line='new')
    print(f'{len(slice_df)=}')


@measure_time
def main():
    """
    Реализуем процесс обработки  обновленных и новых кошельков
    """
    # загрузим обновленные и новые кошельки
    di_5 = csv2df('step_5')
    upd_slice: pd.DataFrame = di_5['upd_slice']
    new_slice: pd.DataFrame = di_5['new_slice']
    assert len(upd_slice) == 405
    assert len(new_slice) == 1549
    
    queue = Queue()

    print('*' * 20 + ' update slice ' + '*' * 40)
    upd_count = 0
    upd_slice.to_csv('data/curr_upd.csv', sep=';', index=False)
    for idx, row in upd_slice.iterrows():
        upd_count += 1
        print(f'upd done = {upd_count}; {row.address}')
    print()
    print('*' * 20 + ' new slice ' + '*' * 40)
    print('finish')


if __name__ == '__main__':
    main()
    # update_new_address()
    # run_thread_pool(10, 'socks')  # запусить обработку с socks прокси
    # requests_with_proxy(0)
    # try_requests_with_http_proxy()
