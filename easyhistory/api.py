# coding:utf-8
from rqalpha.data.base_data_source import BaseDataSource
import pandas as pd
import easyutils
import datetime
import os
from .day import Day


def init(dtype='D', export='csv', path='history'):
    return Day(path=path, export=export).init()


def update_single_code(dtype='D', stock_code=None, path='history', export='csv'):
    if stock_code is None:
        raise Exception('stock code is None')
    return Day(path=path, export=export).update_single_code(stock_code)


def get_stock_codes(dtype='D', export='csv', path='history'):
    return Day(path=path, export=export).get_stock_codes()


def update(dtype='D', export='csv', path='history'):
    return Day(path=path, export=export).update()


def update_index(dtype='D', export='csv', path='history'):
    return Day(path=path, export=export).update_index()


def day2week(dtype='D', export='csv', path='history'):
    return Day(path=path, export=export).day2week()


bundle_path = '~/.rqalpha/bundle'
d = BaseDataSource(os.path.expanduser(bundle_path))
instruments = d._instruments.get_all_instruments()
stock_map = {i.order_book_id: i for i in instruments}

def history(stock_code, market=None, d=d, stock_map=stock_map):
    if not market:
        market = easyutils.get_stock_type(stock_code)
    if market == 'sh':
        stock_code += '.XSHG'
    else:
        stock_code += '.XSHE'
    raw = d._all_day_bars_of(stock_map[stock_code])
    df = pd.DataFrame.from_dict(raw)

    def f(x):
        return(str(int(x['datetime'] / 1000000))[:9])

    df['datetime'] = df.apply(f, axis=1)
    df.columns = ['Date', 'Open', 'Close', 'High', 'Low', 'Volume', 'total_turnover', 'limit_up', 'limit_down']
    df.set_index('Date', inplace=True)
    df.index = pd.to_datetime(df.index)

    return df
