# coding=utf-8

from lib.simulator.base import BacktestSys
import numpy as np
import re
import pandas as pd
from basis_spread_ic import BasisSpread

class StrategyRTN(BacktestSys):
    def __init__(self):
        # super(BasisSpread, self).__init__()
        self.current_file = __file__
        self.prepare()

    def strategy(self):


        rtn_period = 20
        rtn5_dict = {}
        rtn20_dict = {}
        rtn30_dict = {}
        vol_chg = {}
        oi_chg = {}
        vol_ratio = {}
        oi_ratio = {}

        corr_high_low = {}


        wgtsDict = {}

        for nm in self.data:
            wgtsDict[nm] = np.zeros_like(self.dt)
            rtn5_dict[nm] = np.ones_like(self.data[nm]['CLOSE']) * np.nan
            rtn5_dict[nm][5:] = self.data[nm]['CLOSE'][5:] / self.data[nm]['CLOSE'][:-5] - 1.
            rtn20_dict[nm] = np.ones_like(self.data[nm]['CLOSE']) * np.nan
            rtn20_dict[nm][20:] = self.data[nm]['CLOSE'][20:] / self.data[nm]['CLOSE'][:-20] - 1.
            rtn30_dict[nm] = np.ones_like(self.data[nm]['CLOSE']) * np.nan
            rtn30_dict[nm][30:] = self.data[nm]['CLOSE'][30:] / self.data[nm]['CLOSE'][:-30] - 1.

            vol_chg[nm] = np.ones_like(self.data[nm]['VOLUME']) * np.nan
            vol_chg[nm][rtn_period:] = self.data[nm]['VOLUME'][rtn_period:] / self.data[nm]['VOLUME'][:-rtn_period] - 1.
            oi_chg[nm] = np.ones_like(self.data[nm]['OI']) * np.nan
            oi_chg[nm][rtn_period:] = self.data[nm]['OI'][rtn_period:] / self.data[nm]['OI'][:-rtn_period] - 1.

            vol_ratio[nm] = np.ones_like(self.data[nm]['VOLUME']) * np.nan
            temp = pd.DataFrame(self.data[nm]['VOLUME']).rolling(window=5).mean().values.flatten()
            vol_ratio[nm] = self.data[nm]['VOLUME'] / temp

            oi_ratio[nm] = np.ones_like(self.data[nm]['OI']) * np.nan
            temp = pd.DataFrame(self.data[nm]['OI']).rolling(window=5).mean().values.flatten()
            oi_ratio[nm] = self.data[nm]['OI'] / temp

            corr_high_low[nm] = pd.DataFrame(self.data[nm]['LOW']).rolling(window=20, min_periods=15).\
                corr(pd.DataFrame(self.data[nm]['VOLUME'])).values.flatten()



            # rtn20_dict[nm] = rtn20_dict[nm] * vol_ratio[nm]

        # rtn_df = pd.DataFrame.from_dict(rtn_dict, orient='columns')
        # basis_df = pd.DataFrame.from_dict(basis_spread_ratio, orient='columns')
        # basis_df = basis_df.shift(periods=rtn_period)
        # ic_df = rtn_df.corrwith(basis_df, axis=1)
        # ic = ic_df.values.flatten()
        # ic[np.isnan(ic)] = 1.

        fac_dict = corr_high_low
        for i in np.arange(len(self.dt)):

            # 根据基差比例进行交易，多正基差最大的n只，空负基差最小的n只
            fac_daily = []
            for k in fac_dict:
                fac_daily.append(fac_dict[k][i])
            fac_daily = np.array(fac_daily)
            count = len(fac_daily[~np.isnan(fac_daily)])
            if count <= 1:
                continue
            fac_series = fac_daily[~np.isnan(fac_daily)]
            fac_series.sort()
            num_selection = min(3, count / 2)
            low_point = fac_series[num_selection-1]
            high_point = fac_series[-num_selection]


            for k in fac_dict:
                if fac_dict[k][i] <= low_point:
                    wgtsDict[k][i] = -1.
                elif fac_dict[k][i] >= high_point:
                    wgtsDict[k][i] = 1.


        return wgtsDict


if __name__ == '__main__':
    a = StrategyRTN()
    wgtsDict = a.strategy()
    wgtsDict = a.wgtsStandardization(wgtsDict, mode=0)
    wgtsDict = a.wgtsProcess(wgtsDict)

    # b = BasisSpread()
    # wgts_b = b.strategy()
    # wgts_b = a.wgtsStandardization(wgts_b)
    # wgts_b = a.wgtsProcess(wgts_b)
    #
    # con1 = np.in1d(a.dt, b.dt)
    # con2 = np.in1d(b.dt, a.dt)
    # if len(a.dt) == len(b.dt) and con1.all() and con2.all():
    #     final_wgt = {}
    #     for k in wgtsDict:
    #         if k in wgts_b:
    #             final_wgt[k] = 0.5 * wgtsDict[k] + 0.5 * wgts_b[k]
    #         else:
    #             final_wgt[k] = wgtsDict[k]
    #     for k in wgts_b:
    #         if k not in final_wgt:
    #             final_wgt[k] = final_wgt[k]

    a.displayResult(wgtsDict, saveLocal=True)


