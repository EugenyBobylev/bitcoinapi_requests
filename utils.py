import asyncio
import math
import os.path
from datetime import datetime
import json
import time
from datetime import date, timedelta
from functools import wraps
from pathlib import Path

import pandas as pd
import psutil
from pandas import DataFrame
from pytz import utc


class UtcDate:
    """Работа с датами и метками времени в формате UTC"""
    @staticmethod
    def now() -> datetime:
        """Get date time without microseconds"""
        dt = datetime.now(tz=utc)
        now = datetime(year=dt.year, month=dt.month, day=dt.day,
                       hour=dt.hour, minute=dt.minute, second=dt.second, tzinfo=utc)
        return now

    @staticmethod
    def today() -> datetime:
        """Get start today datetime"""
        dt = datetime.now(tz=utc)
        _today = datetime(year=dt.year, month=dt.month, day=dt.day, tzinfo=utc)
        return _today

    @staticmethod
    def add_hours(src_dt, hours=1) -> datetime:
        delta = timedelta(hours=hours)
        dt = src_dt + delta
        return dt

    @staticmethod
    def sub_hours(src_dt, hours=1) -> datetime:
        delta = timedelta(hours=hours)
        dt = src_dt - delta
        return dt

    @staticmethod
    def add_minutes(src_dt, minutes=20):
        delta = timedelta(minutes=minutes)
        dt = src_dt + delta
        return dt

    @staticmethod
    def start_hour(src_dt):
        dt = datetime(year=src_dt.year, month=src_dt.month, day=src_dt.day, hour=src_dt.hour, tzinfo=utc)
        return dt

    @staticmethod
    def sub_minutes(src_dt, minutes=20):
        delta = timedelta(minutes=minutes)
        dt = src_dt - delta
        return dt

    @staticmethod
    def ts_add_hours(src_ts, hours=1):
        ts = src_ts + (hours * 3600)
        return ts

    @staticmethod
    def ts_sub_hours(src_ts, hours=1):
        ts = src_ts - hours * 3600
        return ts

    @staticmethod
    def ts_add_minutes(src_ts, minutes=1):
        ts = src_ts + minutes * 60
        return ts

    @staticmethod
    def ts_sub_minutes(src_ts, minutes=1):
        ts = src_ts - minutes * 60
        return ts

    @staticmethod
    def ts_hour(ts_src: int) -> int:
        """Вернуть метку времени на начало часа"""
        ts = ts_src - ts_src % 3600
        return ts

    @staticmethod
    def dt_hour(dt_src: datetime) -> datetime:
        """Вернуть время на начало часа"""
        ts = UtcDate.dt2ts(dt_src)
        ts = UtcDate.ts_hour(ts)
        dt = UtcDate.ts2dt(ts)
        return dt

    @staticmethod
    def ts_next_hour(ts_src: int) -> int:
        ts = ts_src - (ts_src % 3600) + 3600
        return ts

    @staticmethod
    def dt_next_hour(dt_src: datetime) -> datetime:
        """Вернуть метку времени начала следующего часа"""
        ts = UtcDate.dt2ts(dt_src)
        ts = UtcDate.ts_next_hour(ts)
        dt = UtcDate.ts2dt(ts)
        return dt

    @staticmethod
    def ts_prev_hour(ts_src: int) -> int:
        ts = ts_src - (ts_src % 3600) - 3600
        return ts

    @staticmethod
    def dt_prev_hour(dt_src: datetime) -> datetime:
        """Вернуть метку времения начала предыдущего часа"""
        ts = UtcDate.dt2ts(dt_src)
        ts = UtcDate.ts_prev_hour(ts)
        dt = UtcDate.ts2dt(ts)
        return dt

    @staticmethod
    def dt2ts(src_dt: datetime) -> int:
        """datetime to timestamp"""
        ts = src_dt.timestamp()
        return int(ts)

    @staticmethod
    def dt2binance_ts(src_dt: datetime) -> int:
        """datetime to binances timestamp"""
        return UtcDate.dt2ts(src_dt) * 1000

    @staticmethod
    def ts2dt(src_ts: int) -> datetime | None:
        if src_ts is None:
            return None
        dt = datetime.fromtimestamp(src_ts, tz=utc)
        return dt

    @staticmethod
    def ts2binance_ts(src_ts: int) -> int:
        return src_ts * 1000

    @staticmethod
    def binance_ts2ts(src_ts: int) -> int:
        return src_ts // 1000

    @staticmethod
    def binance_ts2dt(src_ts: int) -> datetime:
        ts = UtcDate.binance_ts2ts(src_ts)
        dt = UtcDate.ts2dt(ts)
        return dt

    @staticmethod
    def dt2str(src_dt: datetime, z='Z'):
        """
        datetime to str
         z == Z --> 2024-02-15 00:00:00UTC
         z == z --> 2024-02-15 00:00:00+0000
         else  ---> 2024-02-15
        """
        frmt = f'%Y-%m-%d %H:%M:%S'
        if z == 'z':
            frmt = f'{frmt}%{z}'
        elif z == 'Z':
            frmt = f'{frmt} %{z}'
        dt_str = src_dt.strftime(frmt)
        return dt_str

    @staticmethod
    def str2dt(dt_str: str, z='Z') -> datetime:
        frmt = '%Y-%m-%d %H:%M:%S'
        if z == 'z':
            frmt = f'{frmt}%{z}'
        elif z == 'Z':
            frmt = f'{frmt} %{z}'
        dt = datetime.strptime(dt_str, frmt)

        if z in ['z', 'Z']:
            dt = datetime(year=dt.year, month=dt.month, day=dt.day,
                          hour=dt.hour, minute=dt.minute, second=dt.second, tzinfo=utc)
        return dt

    @staticmethod
    def str2utc_dt(dt_str: str) -> datetime:
        frmt = '%Y-%m-%d %H:%M:%S'
        dt = datetime.strptime(dt_str, frmt)

        dt = datetime(year=dt.year, month=dt.month, day=dt.day,
                      hour=dt.hour, minute=dt.minute, second=dt.second, tzinfo=utc)
        return dt

    @staticmethod
    def to_utc(src_dt: datetime) -> datetime:
        """local datetiem ti utc datetime"""
        dt = datetime.astimezone(src_dt, tz=utc)
        return dt

    @staticmethod
    def utc2utcz(src_dt: datetime) -> datetime:
        """utc date tieme without time zone info to utc datetime witht time zone info"""
        dt = datetime(year=src_dt.year, month=src_dt.month, day=src_dt.day,
                      hour=src_dt.hour, minute=src_dt.minute, second=src_dt.second, tzinfo=utc)
        return dt

    @staticmethod
    def now_ts(shift: int = 0) -> int:
        """
        Вернуть текущую метку времени кратную параметку shift
        :param shift: в секундах
        :return: метка времени в виде int
        """
        now = UtcDate.now()
        ts = UtcDate.dt2ts(now)
        if shift > 0:
            ts = ts - (ts % shift)
        return ts

    @staticmethod
    def ts2str(src_ts, z: str = 'Z') -> str:
        """timstamp to string datetime like 2024-02-18 12:00:00 UTC"""
        dt = UtcDate.ts2dt(src_ts)
        dt_str = UtcDate.dt2str(dt, z)
        return dt_str


def dumps(obj, fname):
    """Преобразовать в json и записать файл"""
    json_str = json.dumps(obj)
    if obj is not None:
        with open(fname, mode='w', encoding='utf-8') as f:
            f.write(json_str)


def loads(fname):
    """Восстановить объект из json"""
    with open(fname, mode='r', encoding='utf-8') as f:
        _txt = f.read()
    _data = json.loads(_txt)
    return _data


async def read_files(files: list[str]) -> list[str]:
    """Асинхронно читаем несколько файлов с диска и возвращаем список содежащий контент"""
    def sort_key(item):
        return item.get_name()

    for fname in files:
        if not os.path.exists(fname):
            return []

    tasks = []
    for idx, fname in enumerate(files):
        coro = asyncio.to_thread(read_file, fname)
        task = asyncio.create_task(coro, name=str(idx))
        tasks.append(task)
    done, pending = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
    done = sorted(list(done), key=sort_key)
    result = []
    for task in done:
        data = task.result()
        result.append(data)
    return result


def read_file(fname):
    """Читаем файл из файла"""
    ok = os.path.exists(fname)
    if not ok:
        return ''
    with open(fname, mode='r', encoding='utf-8') as f:
        _txt = f.read()
    return _txt


def df2csv(df: DataFrame, filename_csv: str, index=True, mode='w'):
    """
    Write DataFrame to csv file
    :param df: source DataFrame
    :param filename_csv:
    :param index: will save index or not
    :param mode: 'w' - will overwrite the existing CSV file; 'a' - will append DataFrame to existing CSV file
    """
    df.to_csv(filename_csv, index=index, mode=mode)


def csv2df(filename) -> DataFrame | None:
    path = Path(filename)
    if path.exists():
        df = pd.read_csv(filename)
        return df
    return None


def pd_set_display_options(max_columns=None, max_rows=None, max_width=None, min_rows=24):
    """
    Установка максимального количества строк и колонок для печати данных в dataframe
    """
    pd.set_option('display.max_rows', max_rows)
    pd.set_option('display.max_columns', max_columns)
    pd.set_option('display.width', max_width)
    pd.set_option('display.min_rows', min_rows)


def dates_count_down_generator(from_date_str: str, to_date_str: str, count_days: int = 10) -> date:
    """
    Генератор дат от from_date_str до to_date_str включительно в обратном порядке с заданным шагом дней
    """
    from_date = date.fromisoformat(from_date_str)
    to_date = date.fromisoformat(to_date_str)
    if from_date - timedelta(days=count_days) < to_date:
        return from_date

    while from_date > to_date:
        yield from_date
        from_date = from_date - timedelta(days=count_days)
        if from_date <= to_date:
            continue


def slices_data(data: list, cnt: int = 200) -> list[list]:
    """
    Нарезать список содержащий cnt элементов в виде csv строк
    """
    slices = [data[i:i + cnt] for i in range(0, len(data), cnt)]
    return slices


def int2float(int_val: int) -> float:
    """Преобразовать целочисленное значение баланса из формата bitcoin api в доллары и центы"""
    sign = '-' if int_val < 0 else ''
    if int_val < 0:
        int_val = -int_val

    str_int_val = str(int_val)
    str_int_val = f'{sign}{"0" * (9 - len(str_int_val))}{str_int_val}'

    str_float_val = f'{str_int_val[0:-8]}.{str_int_val[-8: ]}'
    float_val = float(str_float_val)
    return float_val


def get_int_count(float_val: float) -> int:
    """Вернуть количество знаков в целой части числа с плавающей точной"""
    cnt = int(math.log10(float_val) + 1)
    return cnt


def process_memory():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss


def measure_mem(func):
    """Декоратор для измерения потребляемой памяти"""
    def wrapper(*args, **kwargs):
        mb = 1024 * 1024
        mem_before = process_memory()
        result = func(*args, **kwargs)
        mem_after = process_memory()
        delta_mem = (mem_after - mem_before) // 1024
        print(f"{func.__name__}:consumed memory: mem_before={mem_before // mb}Mb, mem_after={mem_after // mb}Mb, "
              f"{delta_mem=}kb")
        return result
    return wrapper


def measure_time(func):
    """Декоратор для измерения времени выполнения функции"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        elapsed_time = end - start
        print(f'{func.__name__} took {elapsed_time} sec.')
        return result

    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        start = time.time()
        result = await func(*args, **kwargs)
        end = time.time()
        elapsed_time = end - start
        print(f'{func.__name__} took {elapsed_time} sec.')
        return result

    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    return wrapper
