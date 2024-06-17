import os
from queue import Empty, Queue
from threading import Thread

import pandas as pd
from pandas import Series

my_queue = Queue()


def write(qieue: Queue):
    df: pd.DataFrame = pd.read_csv('curr_upd.csv', sep=';')
    for idx, row in df.iterrows():
        qieue.put(row)


def handle_data(queue: Queue):
    while not queue.empty():
        try:
            row: Series = queue.get()
            print(row.address)
            df = pd.DataFrame([dict(row)])
            # is_exists = os.path.exists('upd_done.csv')
            # df.to_csv('upd_done.csv', sep=';', mode='a', index=False, header=(not is_exists))
        except Empty:
            print('queue is Empty')
            break
    print('done!')


def read(queue: Queue):
    threads = [
        Thread(target=handle_data, args=(queue,)),
        Thread(target=handle_data, args=(queue,)),
    ]

    for thread in threads:
        thread.start()

    if os.path.exists('upd_done.csv'):
        df = pd.read_csv('upd_done.csv', sep=';')
        print(f'{len(df)=}')
    else:
        print('No DataFrame')


if __name__ == '__main__':
    if os.path.exists('upd_done.csv'):
        os.remove('upd_done.csv')
    write(my_queue)
    read(my_queue)
