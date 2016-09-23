# coding: utf8
import json
import os
from datetime import datetime
import talib
import easyutils
import pandas as pd
import numpy as np


def use(export='csv', **kwargs):
    if export.lower() in ['csv']:
        return CSVStore(**kwargs)


class Store:
    def load(self, stock_data):
        pass

    def write(self, stock_code, data):
        pass


class CSVStore(Store):
    def __init__(self, path, dtype):
        self.week_path = os.path.join(path, 'week')
        self.index_path = os.path.join(path, 'index')
        if dtype.lower() in ['d']:
            self.path = os.path.join(path, 'day')
        self.result_path = os.path.join(self.path, 'data')
        self.raw_path = os.path.join(self.path, 'raw_data')

    def write(self, stock_code, updated_data, type='stk'):
        if type == 'stk':
            if not os.path.exists(self.result_path):
                os.makedirs(self.result_path)
            if not os.path.exists(self.raw_path):
                os.makedirs(self.raw_path)

            csv_file_path = os.path.join(self.raw_path, '{}.csv'.format(stock_code))
            if os.path.exists(csv_file_path):
                try:
                    his = pd.read_csv(csv_file_path)
                except ValueError:
                    return
                updated_data_start_date = updated_data[0][0]
                old_his = his[his.Date < updated_data_start_date]
                updated_his = pd.DataFrame(updated_data, columns=his.columns)
                his = old_his.append(updated_his)
            else:
                his = pd.DataFrame(updated_data,
                                   columns=['Date', 'Open', 'High', 'Close', 'Low', 'Volume', 'Amount', 'factor'])
            his.to_csv(csv_file_path, index=False)
            date = his.iloc[-1].Date
            self.write_summary(stock_code, date)
            self.write_factor_his(stock_code, his)
        else:
            csv_file_path = os.path.join(self.index_path, 'index{}.csv'.format(stock_code))
            updated_data.to_csv(csv_file_path, index=False)

    def get_his_stock_date(self, stock_code):
        summary_path = os.path.join(self.raw_path, '{}_summary.json'.format(stock_code))
        with open(summary_path) as f:
            summary = json.load(f)
        latest_date = datetime.strptime(summary['date'], '%Y-%m-%d')
        return latest_date

    def write_summary(self, stock_code, date):
        file_path = os.path.join(self.raw_path, '{}_summary.json'.format(stock_code))
        with open(file_path, 'w') as f:
            latest_day = datetime.strptime(date, '%Y-%m-%d')
            summary = dict(
                    year=latest_day.year,
                    month=latest_day.month,
                    day=latest_day.day,
                    date=date
            )
            json.dump(summary, f)

    def write_factor_his(self, stock_code, his):
        result_file_path = os.path.join(self.result_path, '{}.csv'.format(stock_code))
        his['Adj Close'] = his['Close']
        factor_cols = his.columns.difference(['Date','Volume','Amount']) 
        his[factor_cols] = his[factor_cols] / his.factor.max()
        his.to_csv(result_file_path, index=False)

    def write_week_his(self, stock_code, type = 'stk'):
        if type == 'stk':
            csv_file_path = os.path.join(self.result_path, '{}.csv'.format(stock_code))
            result_file_path = os.path.join(self.week_path, '{}w.csv'.format(stock_code))
            position_file_path = os.path.join(self.week_path, '{}wp.csv'.format(stock_code)) 
        else:
            csv_file_path = os.path.join(self.index_path, 'index{}.csv'.format(stock_code))
            result_file_path = os.path.join(self.index_path, 'index{}w.csv'.format(stock_code))    
            position_file_path = os.path.join(self.index_path, 'index{}wp.csv'.format(stock_code))          

        stock_data = pd.read_csv(csv_file_path)
        period_type = 'W'
        for i in range(len(stock_data)):
            stock_data.iloc[i,0] = datetime.strptime(stock_data.iloc[i,0],"%Y-%m-%d")
        # 将 date 设定为 index
        stock_data.set_index('Date', inplace=True)
        # 进行转换，周线的每个变量都等于那一周最后一个交易日的变量值
        period_stock_data = stock_data.resample(period_type).last()
        # 轴线的 open 等于那一周中第一个交易日的 open
        period_stock_data['Open'] = stock_data['Open'].resample(period_type).first()
        # 周线的 high 等于那一周中 high 的最大值
        period_stock_data['High'] = stock_data['High'].resample(period_type).max()
        # 周线的 low 等于那一周中 low 的最小值
        period_stock_data['Low'] = stock_data['Low'].resample(period_type).min()
        # 轴线的 volume 和 money 等于那一周中 volume 和 money 的各自的和
        period_stock_data['Volume'] = stock_data['Volume'].resample(period_type).sum()
        period_stock_data['Amount'] = stock_data['Amount'].resample(period_type).sum()
        period_stock_data.reset_index(inplace = True)
        period_stock_data.dropna(inplace = True)
        # =============== 导出数据
        period_stock_data.to_csv(result_file_path, index = False)

        # Position mgmt
        close = period_stock_data['Close'].values
        period_stock_data['ma5'] = talib.MA(close,5)
        period_stock_data['ma10'] = talib.MA(close, 10)
        period_stock_data['ma20'] = talib.MA(close, 20)
        period_stock_data['ma20_1'] = period_stock_data['ma20'].shift(1)

        def f(x):
            if (x['Close'] > x['ma5']) & (x['ma5'] > x['ma10']) & (x['ma20'] > x['ma20_1']):
                return 1.0

        period_stock_data['position'] = period_stock_data.apply(f, axis=1)
        # period_stock_data['position'] = np.where((period_stock_data['Close'] > period_stock_data['ma5']) & (period_stock_data['ma5'] > period_stock_data['ma10']) & (period_stock_data['ma20']) > period_stock_data['ma20_1'], 1.0, 0)
        period_stock_data.fillna(0, inplace = True)
        period_stock_data.set_index('Date', inplace = True)
        period_stock_data.drop(['Open','High','Low','Volume','Amount','Adj Close','ma5','ma10','ma20','ma20_1'],axis=1, inplace = True)
        period_stock_data = period_stock_data.resample('D').pad()
        period_stock_data.to_csv(position_file_path, index = True)

    @property
    def init_stock_codes(self):
        stock_codes = easyutils.stock.get_all_stock_codes()
        exists_codes = set()
        if os.path.exists(self.raw_path):
            code_slice = slice(-4)
            exists_codes = {code[code_slice] for code in os.listdir(self.raw_path) if code.endswith('.csv')}
        return set(stock_codes).difference(exists_codes)

    @property
    def update_stock_codes(self):
        code_slice = slice(6)
        return [f[code_slice] for f in os.listdir(self.raw_path) if f.endswith('.json')]
