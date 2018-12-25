# coding=utf-8

import numpy as np
import pandas as pd
import os
import sys
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import inspect
current_file = inspect.getfile(inspect.currentframe())
current_path = os.path.split(current_file)[0]
parent_path = os.path.split(current_path)[0]
sys.path.append(parent_path)

from lib.simulator.base import BacktestSys


class DeviationStrategy(BacktestSys):
    def __init__(self):
        super(DeviationStrategy, self).__init__()
        pd.set_option('display.max_columns', 30)

    def strategy(self):
        self.start_date = '20001001'
        self.end_date = datetime.today()
        self.bt_mode = 'CLOSE'
        self.capital = 1e4

        pta = self.prepareData(db='FuturesDailyWind', collection='TA.CZC_Daily', contract='TA.CZC',
                                    field=['date', 'CLOSE'])
        ll = self.prepareData(db='FuturesDailyWind', collection='L.DCE_Daily', contract='L.DCE',
                                 field=['date', 'CLOSE'])
        pp = self.prepareData(db='FuturesDailyWind', collection='PP.DCE_Daily', contract='PP.DCE',
                              field=['date', 'CLOSE'])


        dt_pta = pta['date']
        cls_pta = pta['CLOSE']
        pta_df = pd.DataFrame({'PTA': cls_pta}, index=dt_pta)

        dt_ll = ll['date']
        cls_ll = ll['CLOSE']
        ll_df = pd.DataFrame({'L': cls_ll}, index=dt_ll)

        total_df = pta_df.join(ll_df, how='outer')
        total_df.dropna(inplace=True)
        total_df['price_diff'] = total_df['L'] - total_df['PTA']
        total_df['capital'] = total_df['L'] + total_df['PTA']

        season_devi = self.season_devi(total_df, rtn_len=60)


        season_devi = season_devi['season_deviation_60days'].values

        dt = total_df.index
        self.start_date = dt[0]
        wgts_ta = np.zeros_like(season_devi)
        wgts_ll = np.zeros_like(season_devi)

        for i in range(1, len(season_devi)):
            wgts_ll[i] = wgts_ll[i-1]
            wgts_ta[i] = wgts_ta[i-1]
            if season_devi[i] > 3:
                wgts_ll[i] = 1.
                wgts_ta[i] = -1.
            elif wgts_ll[i-1] == 1. and wgts_ta[i-1] == -1. and season_devi[i] <= 0.:
                wgts_ll[i] = 0.
                wgts_ta[i] = 0.
            elif season_devi[i] < -3:
                wgts_ll[i] = -1.
                wgts_ta[i] = 1.
            elif wgts_ll[i - 1] == -1. and wgts_ta[i - 1] == 1. and season_devi[i] >= 0.:
                wgts_ll[i] = 0.
                wgts_ta[i] = 0.

        # plt.plot(season_devi)
        # plt.grid()
        #
        #
        #
        # plt.show()

        tr1 = self.stat_trade(db='FuturesDailyWind', collection='L.DCE_Daily', contract='L.DCE', multiplier=5,
                              wgts=wgts_ll)

        tr2 = self.stat_trade(db='FuturesDailyWind', collection='TA.CZC_Daily', contract='TA.CZC', multiplier=5,
                              wgts=wgts_ta)

        nv1 = self.calc_pnl(db='FuturesDailyWind', collection='L.DCE_Daily', contract='L.DCE', multiplier=5,
                            wgts=wgts_ll, tcost_type=0, tcost=5)
        nv2 = self.calc_pnl(db='FuturesDailyWind', collection='TA.CZC_Daily', contract='TA.CZC', multiplier=5,
                            wgts=wgts_ta, tcost_type=0, tcost=5)

        trade1_pnl = np.array([t.pnl for t in tr1])
        trade2_pnl = np.array([t.pnl for t in tr2])
        trade_pnl = trade1_pnl + trade2_pnl

        for n, t in enumerate(tr1):
            print t.__dict__
            print tr2[n].__dict__

        self.net_value = nv1['pnl'].values + nv2['pnl'].values + self.capital

        self.calcBTResult()
        # tr_all = tr1 + tr2
        # self.order_combination(tr_all)
        plt.subplot(211)
        plt.plot_date(nv1.index, self.net_value, fmt='-r', label='PnL')
        plt.grid()
        plt.legend()

        plt.subplot(212)
        plt.hist(trade_pnl, bins=50, label='DistOfPnL', color='r')
        plt.legend()
        plt.grid()

        plt.show()



    def season_devi(self, df, rtn_len):
        # 传入的参数df是dataframe
        price_diff = df[['price_diff', 'capital']].copy()
        price_diff.dropna(inplace=True)

        df_index = price_diff.index
        price_diff['short_date'] = [d.strftime('%m-%d') for d in df_index]
        price_diff['year'] = [d.year for d in df_index]
        year_list = np.array([d.year for d in df_index])
        year_set = np.unique(year_list)

        df_total = price_diff.pivot(index='short_date', columns='year', values='price_diff')
        df_total.fillna(method='ffill', inplace=True)

        # df_total = self.seasonal_generator(df)

        df_mean = df_total.rolling(window=3, min_periods=3, axis=1).mean()
        df_mean = df_mean.shift(periods=1, axis=1)

        df_season = pd.DataFrame()

        for y in year_set:

            df_temp = df_mean[[y]].copy()

            if y % 4 != 0 or (y % 4 == 0 and y % 100 != 0):
                df_temp.drop(index='02-29', axis=0, inplace=True)

            df_temp['dt'] = [datetime.strptime(str(y) + '-' + d, '%Y-%m-%d') for d in df_temp.index]

            if df_season.empty:
                df_season = pd.DataFrame({'season_mean': df_temp[y].values}, index=df_temp['dt'].values)
            else:
                df_season = df_season.append(pd.DataFrame({'season_mean': df_temp[y].values},
                                                          index=df_temp['dt'].values))

        df_season = df_season.rolling(window=rtn_len, min_periods=rtn_len-10).mean()
        df_season = df_season.shift(periods=-rtn_len, axis=0)

        price_diff = price_diff.join(df_season, how='left')
        price_diff['profit'] = price_diff['season_mean'] - price_diff['price_diff']

        # df_col = df.columns.tolist()
        # df_col.remove('price_diff')
        # price_diff['avg_price'] = df[df_col].mean(axis=1)

        price_diff['profit_rate'] = price_diff['profit'] / price_diff['capital']

        price_diff['profit_rate_mean'] = price_diff[['profit_rate']].rolling(window=rtn_len, min_periods=rtn_len-10).mean()
        price_diff['profit_rate_std'] = price_diff[['profit_rate']].rolling(window=rtn_len, min_periods=rtn_len-10).std()

        price_diff['season_deviation_%ddays' % rtn_len] = (price_diff['profit_rate'] - price_diff['profit_rate_mean']) \
                                                        / price_diff['profit_rate_std']
        # print price_diff[['price_diff', 'season_deviation_%ddays' % rtn_len]]

        return price_diff[['price_diff', 'season_deviation_%ddays' % rtn_len]]


    # def seasonal_generator(self, df):
    #     # 将时间序列转成季节性矩阵
    #     price_diff = df[['price_diff']].copy()
    #     price_diff.dropna(inplace=True)
    #     df_index = price_diff.index
    #     price_diff['short_date'] = [d.strftime('%m-%d') for d in df_index]
    #     price_diff['year'] = [d.year for d in df_index]
    #
    #     year_list = np.array([d.year for d in df_index])
    #     year_set = np.unique(year_list)
    #
    #     dt_list = self.date_list()
    #
    #     df_total = pd.DataFrame()
    #     for y in year_set:
    #         con = year_list == y
    #         df_year = price_diff.iloc[con]
    #         con = np.in1d(dt_list, df_year['short_date'].values)
    #         temp = np.ones(len(dt_list)) * np.nan
    #         temp[con] = df_year['price_diff'].values
    #         if df_total.empty:
    #             df_total = pd.DataFrame({y: temp}, index=dt_list)
    #         else:
    #             df_total = df_total.join(pd.DataFrame({y: temp}, index=dt_list), how='outer')
    #
    #     df_total.fillna(method='ffill', inplace=True)
    #
    #     return df_total
    #
    # @staticmethod
    # def date_list():
    #     """用于生成日期的list，个数为366个，考虑了闰年的情况"""
    #     d_list = []
    #     dt = datetime(2016, 1, 1)
    #     while dt.year == 2016:
    #         d_list.append(dt.strftime('%m-%d'))
    #         dt = dt + timedelta(1)
    #     return d_list

if __name__ == '__main__':
    a = DeviationStrategy()
    a.strategy()



