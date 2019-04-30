#coding=utf-8
import numpy as np
import pandas as pd
import pymongo
from datetime import datetime


class FacGenerator(object):

    cmd_list = ['L.DCE', 'PP.DCE', 'MA.CZC', 'TA.CZC', 'BU.SHF', 'V.DCE', 'EG.DCE', 'ZC.CZC', 'JM.DCE', 'RB.SHF',
                'J.DCE', 'I.DCE', 'HC.SHF']

    def __init__(self):
        conn = pymongo.MongoClient(host='192.168.1.172', port=27017)
        db = conn['CBNB']
        db.authenticate(name='yuanjie', password='yuanjie')
        self.futures_coll = db['FuturesMD']

        # 高开低收量仓的dataframe

        self.cls = pd.DataFrame()
        self.opn = pd.DataFrame()
        self.high = pd.DataFrame()
        self.low = pd.DataFrame()
        self.volume = pd.DataFrame()
        self.oi = pd.DataFrame()

        for c in self.cmd_list:
            queryArgs = {'wind_code': c, 'date': {'$gte': datetime(2000, 1, 1), '$lte': datetime(2018, 12, 31)}}
            projectionField = ['date', 'CLOSE', 'OPEN', 'HIGH', 'LOW', 'VOLUME', 'OI']
            res = self.futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
            fut_df = pd.DataFrame.from_records(res, index='date')
            fut_df.drop(columns=['_id'], inplace=True)
            self.cls = self.cls.join(fut_df[['CLOSE']], how='outer')
            self.cls.rename(columns={'CLOSE': c}, inplace=True)
            self.opn = self.opn.join(fut_df[['OPEN']], how='outer')
            self.opn.rename(columns={'OPEN': c}, inplace=True)
            self.high = self.high.join(fut_df[['HIGH']], how='outer')
            self.high.rename(columns={'HIGH': c}, inplace=True)
            self.low = self.low.join(fut_df[['LOW']], how='outer')
            self.low.rename(columns={'LOW': c}, inplace=True)
            self.volume = self.volume.join(fut_df[['VOLUME']], how='outer')
            self.volume.rename(columns={'VOLUME': c}, inplace=True)
            self.oi = self.oi.join(fut_df[['OI']], how='outer')
            self.oi.rename(columns={'OI': c}, inplace=True)
        self.rtn = self.cls.pct_change()

    # 排序
    def rank(self, df):
        return df.rank(axis=1)

    # period前的数据
    def delay(self, df, period):
        return df.shift(periods=period)

    # 计算两个dataframe的滚动相关性
    def correlation(self, df1, df2, period):
        return df1.rolling(window=period, min_periods=period-10).corr(df2)

    # 计算两个dataframe的滚动协方差
    def covariance(self, df1, df2, period):
        return df1.rolling(window=period, min_periods=period-10).cov(df2)

    # 进行等比例的放大或缩小
    def scale(self, df, scale_value=1):
        sum_df = df.sum(axis=1)
        res = pd.DataFrame()
        for c in df:
            res[c] = df[c] / sum_df * scale_value
        return res


    def rolling_mean(self, df, period=10):
        return df.rolling(window=period, min_periods=period-10).mean()

fg = FacGenerator()
print fg.scale(fg.volume)










