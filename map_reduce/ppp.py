import pickle
import time

import pandas as pd

data = {
    'one': 1,
    'two': 2,
    1: 'one',
    2: 'two'
}

if __name__ == '__main__':
    print(data)
    print('*' * 100)

    # my_data = pickle.dumps(data)
    # with open('my_data.bin', 'wb') as f:
    #     f.write(my_data)

    with open('my_data.bin', 'rb') as f:
        bin_data = f.read()
    data_dst = pickle.loads(bin_data)
    print(data_dst)
    print('*' * 100)

    # df: pd.DataFrame = pd.read_csv('curr_upd.csv', sep=';')
    # df.to_pickle('curr_upd.bin')

    df = pd.read_pickle('curr_upd.bin')
    print(len(df))
