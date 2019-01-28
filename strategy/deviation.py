# coding=utf-8
from lib.simulator.base import BacktestSys
import numpy as np
import pandas as pd
from datetime import datetime
import re

class Deviation(BacktestSys):
    def __init__(self):
        super(Deviation, self).__init__()

    def strategy(self):

        formulas = [('VAR1 - VAR2', ('L.DCE', 'PP.DCE'))]

        wgtsDict = {}

        for f, v in formulas:
            ptn = re.compile('VAR\d+')
            res = ptn.findall(f)
            if len(res) != len(v):
                raise Exception(u'公式提供错误')
            cls_df = pd.DataFrame(index=self.dt)
            for i in np.arange(len(v)):
                if v[i] not in wgtsDict:
                    wgtsDict[v[i]] = np.zeros_like(self.dt)
                cls_df[v[i]] = self.data[v[i]]['CLOSE']
                f = f.replace(res[i], 'cls_df["%s"]' % v[i])
            print f
            cls_df['price_diff'] = eval(f)
            cls_df['mon_day'] = [d.strftime('%m-%d') for d in self.dt]
            cls_df['year'] = [d.strftime('%Y') for d in self.dt]
            season_df = cls_df.pivot(index='mon_day', columns='year', values='price_diff')
            season_df.fillna(method='ffill', inplace=True)
            season_df = season_df.rolling(window=5, min_periods=3, axis=1).mean()
            yr = max(int(y) for y in season_df.columns) + 1
            season_df[str(yr)] = np.nan
            season_df = season_df.shift(axis=1)
            future_df = pd.DataFrame()
            for c in season_df:
                temp_idx = []
                temp_value = season_df[c]
                for d in season_df.index:
                    try:
                        temp_idx.append(datetime.strptime('%s-%s' % (c, d), '%Y-%m-%d'))
                    except ValueError:
                        temp_value.drop(index=d, inplace=True)
                temp = pd.DataFrame({'price_diff_f': temp_value.values}, index=temp_idx)
                future_df = pd.concat([future_df, temp], axis=0)
            future_df.fillna(method='ffill', inplace=True, limit=3)
            future_df = future_df.rolling(window=20).mean()
            future_df = future_df.shift(periods=-20, axis=0)

            cls_df = cls_df.join(future_df, how='left')
            cls_df['rtn'] = cls_df['price_diff'] - cls_df['price_diff_f']
            cls_df['rtn_std'] = cls_df['rtn'].rolling(window=20).std()
            cls_df['rtn_mean'] = cls_df['rtn'].rolling(window=20).mean()
            cls_df['rtn_standard'] = (cls_df['rtn'] - cls_df['rtn_mean']) / cls_df['rtn_std']
            rtn_standard = cls_df['rtn_standard'].values.flatten()

            for i in np.arange(1, len(self.dt)):
                for j in np.arange(len(v)):
                    ptn_sub = '(\+|-)(?=.*?cls_df\["%s"\])' % v[j]
                    # ptn_add = '+(?=.*?cls_df\["%s"\])' % v[j]
                    print f
                    res = re.compile(ptn_sub)
                    print res.search(f)
                    if res.search(f):
                        sign_v = res.search(f).group()
                    else:
                        sign_v = '+'
                    print sign_v, v[j]

                if rtn_standard[i] >= 3 and wgtsDict[k][i-1] == 0:
                    wgtsDict[k][i] = 1
                elif rtn_standard[i] <= -2.5 and wgtsDict[k][i-1] == 0:
                    wgtsDict[k][i] = -1
                elif rtn_standard[i] < 0 and wgtsDict[k][i-1] == -1:
                    wgtsDict[k][i] = 0
                elif rtn_standard[i] > 0 and wgtsDict[k][i-1] == 1:
                    wgtsDict[k][i] = 0
                else:
                    wgtsDict[k][i] = wgtsDict[k][i-1]


        # for k in self.data:
        #     wgtsDict[k] = np.zeros_like(self.dt)
        #     cls = self.data[k]['CLOSE']
        #     cls_df = pd.DataFrame({'CLOSE': cls}, index=self.dt)
        #     rtn = np.ones_like(cls) * np.nan
        #     rtn[1:] = cls[1:] / cls[:-1] - 1.
        #     cls_df['RTN'] = rtn
        #     cls_df['mon_day'] = [d.strftime('%m-%d') for d in self.dt]
        #     cls_df['year'] = [d.strftime('%Y') for d in self.dt]
        #     season_df = cls_df.pivot(index='mon_day', columns='year', values='CLOSE')
        #     season_df.fillna(method='ffill', inplace=True)
        #     season_df = season_df.rolling(window=5, min_periods=3, axis=1).mean()
        #     yr = max(int(y) for y in season_df.columns) + 1
        #     season_df[str(yr)] = np.nan
        #     season_df = season_df.shift(axis=1)
        #     future_df = pd.DataFrame()
        #     for c in season_df:
        #         temp_idx = []
        #         temp_value = season_df[c]
        #         for d in season_df.index:
        #             try:
        #                 temp_idx.append(datetime.strptime('%s-%s' % (c, d), '%Y-%m-%d'))
        #             except ValueError:
        #                 temp_value.drop(index=d, inplace=True)
        #
        #         temp = pd.DataFrame({'RTN_F': temp_value.values}, index=temp_idx)
        #         future_df = pd.concat([future_df, temp], axis=0)
        #     future_df.fillna(method='ffill', inplace=True, limit=3)
        #     future_df = future_df.rolling(window=20).mean()
        #     future_df = future_df.shift(periods=-20, axis=0)
        #
        #     cls_df = cls_df.join(future_df, how='left')
        #     cls_df['rtn'] = cls_df['RTN'] / cls_df['RTN_F'] - 1
        #     cls_df['rtn_std'] = cls_df['rtn'].rolling(window=20).std()
        #     cls_df['rtn_mean'] = cls_df['rtn'].rolling(window=20).mean()
        #     cls_df['rtn_standard'] = (cls_df['rtn'] - cls_df['rtn_mean']) / cls_df['rtn_std']
        #     rtn_standard = cls_df['rtn_standard'].values.flatten()
        #
        #     for i in np.arange(1, len(self.dt)):
        #         if rtn_standard[i] >= 2.5 and wgtsDict[k][i-1] == 0:
        #             wgtsDict[k][i] = 1
        #         elif rtn_standard[i] <= -2.5 and wgtsDict[k][i-1] == 0:
        #             wgtsDict[k][i] = -1
        #         elif rtn_standard[i] < 0 and wgtsDict[k][i-1] == -1:
        #             wgtsDict[k][i] = 0
        #         elif rtn_standard[i] > 0 and wgtsDict[k][i-1] == 1:
        #             wgtsDict[k][i] = 0
        #         else:
        #             wgtsDict[k][i] = wgtsDict[k][i-1]

        return wgtsDict


if __name__ == '__main__':
    a = Deviation()
    wgt = a.strategy()
    wgt = a.wgtsStandardization(wgt)
    wgt = a.wgtsProcess(wgt)

    a.displayResult(wgt)

