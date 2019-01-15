# coding=utf-8
from lib.simulator.base import BacktestSys
import re
import matplotlib.pyplot as plt
import numpy as np
import pprint
import pandas as pd
import talib

class BasisSpreadAnalysis(BacktestSys):
    def __init__(self):
        super(BasisSpreadAnalysis, self).__init__()

    def analysis(self):
        cmd_dict = {}
        for k in self.category:
            cmd_dict.setdefault(self.category[k], []).append(k)

        stats_df = pd.DataFrame()
        basis = dict()
        rtn = dict()
        rtn_period = 20
        for cmd in cmd_dict.keys():
            ptn = re.compile('_MC_LastMonthEnd')
            for nm in cmd_dict[cmd]:
                if ptn.search(nm):
                    futures = self.data[nm]['CLOSE']
                    remains = self.data[nm]['remain_days']
                    rtn[cmd] = np.ones_like(futures) * np.nan
                    rtn[cmd][:-rtn_period] = futures[rtn_period:] / futures[:-rtn_period] - 1.
                else:
                    spot = self.data[nm].values()[0]
            # spot = pd.DataFrame(spot).fillna(method='ffill').values.flatten()
            # slope_period = 20
            # spot_slope = talib.LINEARREG_SLOPE(spot, timeperiod=slope_period)
            # fut_slope = talib.LINEARREG_SLOPE(futures, timeperiod=slope_period)
            # slope_diff = spot_slope - fut_slope
            basis[cmd] = 1 - futures / spot

        basis_df = pd.DataFrame.from_dict(basis)
        basis_df.index = self.dt
        rtn_df = pd.DataFrame.from_dict(rtn)
        rtn_df.index = self.dt
        rtn_df = rtn_df[basis_df.columns]
        basis_rank = basis_df.rank(axis=1)
        rtn_rank = rtn_df.rank(axis=1)
        ic_rank = basis_rank.corrwith(rtn_rank, axis=1)
        ic = basis_df.corrwith(rtn_df, axis=1)
        ic_stats = ic.rolling(window=20).mean() / ic.rolling(window=20).std()
        ic_rank_stats = ic_rank.rolling(window=20).mean() / ic_rank.rolling(window=20).std()

        rtn_num = 3
        rtn_high = rtn_df.copy()
        rtn_high[basis_df.rank(axis=1, ascending=False) > rtn_num] = np.nan
        rtn_high_avg = rtn_high.mean(axis=1)

        rtn_low = rtn_df.copy()
        rtn_low[basis_df.rank(axis=1, ascending=True) > rtn_num] = np.nan
        rtn_low_avg = rtn_low.mean(axis=1)

        basis_rtn = rtn_high_avg - rtn_low_avg

        print 'IC_MEAN:', np.mean(ic)
        print 'FAC_RTN:', np.mean(basis_rtn)

        plt.figure()
        plt.plot_date(self.dt, basis_rtn.values.flatten(), fmt='-r', label='BasisRtn')
        plt.grid()
        plt.legend()

        plt.figure()
        plt.plot_date(self.dt, ic_stats.values.flatten(), fmt='-r', label='%s-days ic' % rtn_period)
        plt.plot_date(self.dt, ic_rank_stats.values.flatten(), fmt='-k', label='%s-days rank ic' % rtn_period)
        plt.grid()
        plt.legend()

        plt.show()






if __name__ == '__main__':
    a = BasisSpreadAnalysis()
    a.analysis()