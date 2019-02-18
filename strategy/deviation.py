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

        formulas = [('VAR1 - VAR2', ('L.DCE', 'PP.DCE')),
                    ('VAR1 - VAR2', ('L.DCE', 'TA.CZC')),
                    ('VAR1 - 1.2 * VAR2 - 50', ('J.DCE', 'JM.DCE')),
                    ('VAR1 - VAR2', ('L.DCE', 'V.DCE')),
                    ('1./3. * VAR3 - 2 * VAR2 + 1.85 * VAR1 + 637', ('ZC.CZC', 'MA.CZC', 'PP.DCE')),
                    ('VAR1 - 3 * VAR2', ('PP.DCE', 'MA.CZC')),
                    ('VAR1 - 1.85 * VAR2 - 637', ('MA.CZC', 'ZC.CZC')),
                    ('VAR1 - VAR2', ('PP.DCE', 'V.DCE')),
                    ('VAR1 - VAR2', ('TA.CZC', 'RU.SHF')),
                    ('VAR1 - VAR2', ('TA.CZC', 'BU.SHF')),
                    ('VAR1 - 2 * VAR2', ('V.DCE', 'J.DCE')),
                    ('VAR1 - 1.7 * VAR3 - 0.5 * VAR2 - 800', ('RB.SHF', 'J.DCE', 'I.DCE')),
                    ('VAR1 - VAR2', ('HC.SHF', 'RB.SHF')),
                    ('VAR1 - 0.95 * VAR2 - 1000', ('HC.SHF', 'J.DCE')),
                    ('VAR1 - 3.5 * VAR2 - 800', ('RB.SHF', 'I.DCE')),
                    ('VAR1 - VAR2', ('J.DCE', 'ZC.CZC')),
                    ('VAR1 - VAR2', ('BU.SHF', 'FU.SHF'))]

        wgtsDict = {}

        for f, v in formulas:
            wgts_formulas = {}
            ptn = re.compile('VAR\d+')
            res = ptn.findall(f)
            if len(res) != len(v):
                raise Exception(u'公式提供错误')
            cls_df = pd.DataFrame(index=self.dt)
            for i in np.arange(len(v)):
                if v[i] not in wgtsDict:
                    wgtsDict[v[i]] = np.zeros_like(self.dt)
                if v[i] not in wgts_formulas:
                    wgts_formulas[v[i]] = np.zeros_like(self.dt)
                cls_df[v[i]] = self.data[v[i]]['CLOSE']
                f = f.replace(res[i], 'cls_df["%s"]' % v[i])
            cls_df['price_diff'] = eval(f)
            cls_df['mon_day'] = [d.strftime('%m-%d') for d in self.dt]
            cls_df['year'] = [d.strftime('%Y') for d in self.dt]
            season_df = cls_df.pivot(index='mon_day', columns='year', values='price_diff')
            season_df.fillna(method='ffill', inplace=True)
            season_df = season_df.rolling(window=3, min_periods=3, axis=1).mean()
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

            for j in np.arange(len(v)):
                ptn_str = '[+-](?=[0-9\. \*/]*?cls_df\["%s"\])' % v[j]
                ptn = re.compile(ptn_str)
                if ptn.search(f):
                    sign_v = ptn.search(f).group()
                else:
                    sign_v = '+'
                print v[j], sign_v

                for i in np.arange(1, len(self.dt)):
                    if rtn_standard[i] >= 3. and wgts_formulas[v[j]][i-1] == 0:
                        wgts_formulas[v[j]][i] = -1 * int(sign_v + '1')
                    elif rtn_standard[i] <= -3. and wgts_formulas[v[j]][i-1] == 0:
                        wgts_formulas[v[j]][i] = 1 * int(sign_v + '1')
                    elif rtn_standard[i] < 0 and wgts_formulas[v[j]][i-1] == -1 * int(sign_v + '1'):
                        wgts_formulas[v[j]][i] = 0.
                    elif rtn_standard[i] > 0 and wgts_formulas[v[j]][i-1] == 1 * int(sign_v + '1'):
                        wgts_formulas[v[j]][i] = 0.
                    else:
                        wgts_formulas[v[j]][i] = wgts_formulas[v[j]][i-1]

                wgtsDict[v[j]] = wgtsDict[v[j]] + wgts_formulas[v[j]]


        return wgtsDict


if __name__ == '__main__':
    a = Deviation()
    wgt = a.strategy()
    wgt = a.wgtsStandardization(wgt)
    wgt = a.wgtsProcess(wgt)

    a.displayResult(wgt)

