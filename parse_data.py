import os
import pandas as pd
from pytdx.reader import TdxDailyBarReader, TdxFileNotFoundException
import numpy as np
from collections import defaultdict
from datetime import datetime
import math


class ParseData:
    def __init__(self, tdx_path, path, from_date, to_date):
        self.reader = TdxDailyBarReader()
        self.tdx_path = tdx_path
        self.path = path
        self.from_date = datetime.strptime(from_date, '%Y%m%d')
        self.to_date = datetime.strptime(to_date, '%Y%m%d')
        super().__init__()  # 单继承，只有一个父类

    def read_parse(self, path, code):
        dt = np.dtype([
            ('Date', 'u4'),
            ('Open', 'u4'),
            ('High', 'u4'),
            ('Low', 'u4'),
            ('Close', 'u4'),
            ('Amount', 'f'),
            ('Volume', 'u4'),
            ('Reserve', 'u4')])
        data = np.fromfile(path, dtype=dt)
        # df = pd.DataFrame(data)
        # Or if you want to explicitly set the column names
        df = pd.DataFrame(data, columns=data.dtype.names)
        df['code'] = code
        df.eval('''
                year=floor(Date/10000)
                month=floor((Date%10000)/100)
                day=floor(Date%10000%100)
                Open=Open/100
                High=High/100
                Low=Low/100
                Close=Close/100
                ''',
                inplace=True)
        df['time'] = pd.to_datetime(df.loc[:, ['year', 'month', 'day']])
        data = []
        for index, r in df.iterrows():
            if self.from_date <= r['time'] <= self.to_date:
                yu1_1, yu1_2, yu1_3 = self.count_yushu(code[2:], r['Open'])
                yu2_1, yu2_2, yu2_3 = self.count_yushu(code[2:], r['High'])
                yu3_1, yu3_2, yu3_3 = self.count_yushu(code[2:], r['Low'])
                yu4_1, yu4_2, yu4_3 = self.count_yushu(code[2:], r['Close'])
                data.append([code, r['time'],  r['Open'],  r['High'],  r['Low'], r['Close'], int(yu1_3), int(yu2_3),
                             int(yu3_3), int(yu4_3)])
        if not data:
            raise Exception('df为空')
        df = pd.DataFrame(data, columns=['代码', '日期', '开盘价', '最高价', '最低价', '收盘价', '开盘价余数3',
                                         '最高价余数3', '最低价余数3', '收盘价余数3'])
        df.set_index('日期', inplace=True)
        df_week = df.resample('W').last()
        # df_week['代码'] = df['代码'].resample('W').first()
        # df_week['日期'] = df['日期'].resample('W').last()
        df_week['开盘价'] = df['开盘价'].resample('W').first()
        df_week['最高价'] = df['最高价'].resample('W').max()
        df_week['最低价'] = df['最低价'].resample('W').min()
        df_week['收盘价'] = df['收盘价'].resample('W').last()
        df_week = df_week[df_week['代码'].notnull()]
        df_week['开盘价余数3'] = df_week.开盘价.apply(lambda x: self.count_yushu(code[2:], x)[2])
        df_week['最高价余数3'] = df_week.最高价.apply(lambda x: self.count_yushu(code[2:], x)[2])
        df_week['最低价余数3'] = df_week.最低价.apply(lambda x: self.count_yushu(code[2:], x)[2])
        df_week['收盘价余数3'] = df_week.收盘价.apply(lambda x: self.count_yushu(code[2:], x)[2])

        df_week.reset_index(inplace=True)
        df_week[['开盘价余数3', '最高价余数3', '最低价余数3', '收盘价余数3']] = df_week[['开盘价余数3', '最高价余数3', '最低价余数3', '收盘价余数3']].astype(int)
        # print(df_week)
        return df, df_week

    def handle_data(self):
        day_data, week_data = [], []
        folders = ['sz', 'sh', 'bj']
        # folders = ['sz']
        for f in folders:
            final_url = os.path.join(self.tdx_path, 'vipdoc', f, 'lday')
            filenames = os.listdir(final_url)
            for filename in filenames:
                try:
                    df, week_df = self.read_parse(os.path.join(final_url, filename), filename.split('.')[0])
                    day_data.append(df)
                    week_data.append(week_df)
                except Exception as e:
                    print(e)
                print(filename)
        all_day_df = pd.concat(day_data)
        all_week_df = pd.concat(week_data)

        self.split_to_excel(all_day_df, os.path.join(self.path, '日线数据.xlsx'))
        self.split_to_excel(all_week_df, os.path.join(self.path, '周线数据.xlsx'))
        # all_day_df.to_excel(os.path.join(self.path, '日线数据.xlsx'))
        # all_week_df.to_excel(os.path.join(self.path, '周线数据.xlsx'))

        self.get_other_excel(all_day_df, '[日线]')
        self.get_other_excel(all_week_df, '[周线]')

    def count_yushu(self, stock_id, price):
        yu1, yu2, yu3 = 0, 0, 0
        yu1 = (self.count_stock_id(stock_id) + self.count_stock_id(self.numSplit(price)[0])) % 8
        yu2 = (self.count_stock_id(stock_id) + self.count_stock_id(self.numSplit(price)[1])) % 8
        yu3 = (self.count_stock_id(stock_id) + self.count_stock_id(self.numSplit(price)[0]) + self.count_stock_id(self.numSplit(price)[1])) % 6
        return yu1, yu2, yu3

    def split_to_excel(self, df, path):
        row_num = int(df.shape[0])
        split_num = 1000000
        n = math.ceil(row_num / split_num)
        for i in range(n):
            handle_df = df.iloc[split_num * i: split_num * (i + 1)]
            handle_df.to_excel(path.split('.')[0] + '_' + str(i+1) + '.xlsx')

    def numSplit(self, num):
        '''
        浮点数字整数、小数分离【将数字转化为字符串处理】
        '''
        try:
            zs,xs=str(num).split('.')
            return zs, xs
        except Exception as e:
            print(e)
            print(f'exception num:{num}')

    def count_stock_id(self, stock_id):
        res = 0
        for s in stock_id:
            res += int(s)
        return res

    def get_other_excel(self, df, kind):
        # 统计一下每个股的四个价格的第三个余数，统计两个以上的数量
        d1, d4, d5, d_total = defaultdict(int), defaultdict(int), defaultdict(int), defaultdict(int)
        for index, row in df.iterrows():
            # '开盘价余数3', '最高价余数3', '最低价余数3', '收盘价余数3'
            d1_temp, d4_temp, d5_temp = 0, 0, 0
            if row['开盘价余数3'] == 1:
                d1_temp += 1
            if row['最高价余数3'] == 1:
                d1_temp += 1
            if row['最低价余数3'] == 1:
                d1_temp += 1
            if row['收盘价余数3'] == 1:
                d1_temp += 1
            if d1_temp >= 2:
                d1[row['代码']] += 1

            if row['开盘价余数3'] == 4:
                d4_temp += 1
            if row['最高价余数3'] == 4:
                d4_temp += 1
            if row['最低价余数3'] == 4:
                d4_temp += 1
            if row['收盘价余数3'] == 4:
                d4_temp += 1
            if d4_temp >= 2:
                d4[row['代码']] += 1

            if row['开盘价余数3'] == 5:
                d5_temp += 1
            if row['最高价余数3'] == 5:
                d5_temp += 1
            if row['最低价余数3'] == 5:
                d5_temp += 1
            if row['收盘价余数3'] == 5:
                d5_temp += 1
            if d5_temp >= 2:
                d5[row['代码']] += 1

            d_total[row['代码']] = d1[row['代码']] + d4[row['代码']] + d5[row['代码']]

        df_1 = pd.DataFrame([], columns=['代码', '余数两个1及以上的次数'])
        for k in d1:
            df_1 = df_1.append({'代码': k, '余数两个1及以上的次数': d1[k]}, ignore_index=True)
        df_1.sort_values(by=['余数两个1及以上的次数'], ascending=False, inplace=True)
        self.split_to_excel(df_1, os.path.join(self.path,  kind + '余数两个1及以上的次数并降序排列.xlsx'))
        # df_1.to_excel(os.path.join(self.path,  kind + '余数两个1及以上的次数并降序排列.xlsx'))

        df_4 = pd.DataFrame([], columns=['代码', '余数两个4及以上的次数'])
        for k in d4:
            df_4 = df_4.append({'代码': k, '余数两个4及以上的次数': d4[k]}, ignore_index=True)
        df_4.sort_values(by=['余数两个4及以上的次数'], ascending=False, inplace=True)
        self.split_to_excel(df_4, os.path.join(self.path, kind + '余数两个4及以上的次数并降序排列.xlsx'))
        # df_4.to_excel(os.path.join(self.path, kind + '余数两个4及以上的次数并降序排列.xlsx'))

        df_5 = pd.DataFrame([], columns=['代码', '余数两个5及以上的次数'])
        for k in d5:
            df_5 = df_5.append({'代码': k, '余数两个5及以上的次数': d5[k]}, ignore_index=True)
        df_5.sort_values(by=['余数两个5及以上的次数'], ascending=False, inplace=True)
        self.split_to_excel(df_5, os.path.join(self.path, kind + '余数两个5及以上的次数并降序排列.xlsx'))
        # df_5.to_excel(os.path.join(self.path, kind + '余数两个5及以上的次数并降序排列.xlsx'))

        # 总数
        df2 = pd.DataFrame([], columns=['代码', '余数总数次数'])

        for k in d_total:
            df2 = df2.append({'代码': k, '余数总数次数': d_total[k]}, ignore_index=True)
        df2.sort_values(by=['余数总数次数'], ascending=False, inplace=True)
        self.split_to_excel(df2, os.path.join(self.path, kind + '余数总数次数并降序排列.xlsx'))
        # df2.to_excel(os.path.join(self.path, kind + '余数总数次数并降序排列.xlsx'))


# p = ParseData()
# p.handle_data(r'E:\new_tdx')
if __name__=="__main__":
    t = ParseData(r'E:\new_tdx', r'D:\Project\外包\20230126_股票数据处理3', '20090625', '20221031')
    t.handle_data()
