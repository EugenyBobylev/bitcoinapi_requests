import time
from multiprocessing import Process, Pool
from multiprocessing.pool import AsyncResult


def count(count_to: int) -> int:
    start = time.time()

    counter = 0
    while counter < count_to:
        counter += 1

    end = time.time()
    print(f'Рассчет закончен до {count_to} за время: {end - start}')
    return counter


def simple_mp_process():
    start = time.time()
    to_one_hundred_million = Process(target=count, args=(100000000,))
    to_two_hundred_million = Process(target=count, args=(200000000,))

    to_one_hundred_million.start()
    to_two_hundred_million.start()

    to_one_hundred_million.join()
    to_two_hundred_million.join()

    end = time.time()
    print(f'Полное время работц: {end - start}')


def use_mp_pool():
    """"""
    start = time.time()

    with Pool(2) as pool:
        to_one_hundred_million: AsyncResult = pool.apply_async(count, args=(100000000,))
        to_two_hundred_million: AsyncResult = pool.apply_async(count, args=(200000000,))

        res_1: int = to_one_hundred_million.get()
        print(f'to_one_hundred_million = {res_1}')

        res_2: int = to_two_hundred_million.get()
        print(f'to_two_hundred_million = {res_2}')

    end = time.time()
    print(f'Полное время работц: {end - start}')


if __name__ == '__main__':
    # simple_mp_process()
    use_mp_pool()
