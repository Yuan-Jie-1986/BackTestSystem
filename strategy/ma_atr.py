# coding=utf-8

from lib.simulator.base import BacktestSys
import numpy as np
import re
import pandas as pd

class MA_ATR(BacktestSys):
    def __init__(self):
        super(MA_ATR, self).__init__()

    def strategy(self):

        wgtsDict = {}
        for k in self.data:
            wgtsDict[k] = np.zeros_like(self.dt)
            cls = self.data[k]['CLOSE']
            ma20 = pd.DataFrame(cls).rolling(window=20).mean().values.flatten()
            ma10 = pd.DataFrame(cls).rolling(window=10).mean().values.flatten()

            con = np.zeros(cls.shape)
            con[cls > ma20] = 10
            con[(cls < ma10) * (cls > ma20)] = 0
            con[cls < ma20] = -10
            con[(cls > ma10) * (cls < ma20)] = 0

            wgtsDict[k] = con


        # for i in np.arange(len(self.dt)):
        #
        #     bsr_daily = []
        #     for k in basis_spread_ratio:
        #         bsr_daily.append(basis_spread_ratio[k][i])
        #     bsr_daily = np.array(bsr_daily)
        #     count = len(bsr_daily[~np.isnan(bsr_daily)])
        #     if count <= 1:
        #         continue
        #     bsr_series = bsr_daily[~np.isnan(bsr_daily)]
        #     bsr_series.sort()
        #     num_selection = min(3, count / 2)
        #     low_point = bsr_series[num_selection-1]
        #     high_point = bsr_series[-num_selection]
        #
        #     # 计算得到各合约的20日波动
        #     vol_daily = {}
        #     wgt_daily = {}
        #     n = max(0, i - 240)
        #     for k in basis_spread_ratio:
        #         vol_daily[k] = np.std(self.data[k]['CLOSE'][n:i]) * self.unit[self.category[k]]
        #         # vol_daily[k] = self.data[k]['CLOSE'][i] * self.unit[self.category[k]]
        #         wgt_daily[k] = 1. / vol_daily[k]
        #         # if np.isinf(wgt_daily[k]):
        #         #     print wgt_daily[k], k, vol_daily[k]
        #
        #     wgt_min = np.nanmin(wgt_daily.values())
        #     for k in wgt_daily:
        #         wgt_daily[k] = wgt_daily[k] / wgt_min
        #         if ~np.isfinite(wgt_daily[k]):
        #             wgt_daily[k] = 0.
        #         # if np.isinf(wgt_daily[k]):
        #         #     wgt_daily[k] = 1.
        #
        #
        #     for k in basis_spread_ratio:
        #         if basis_spread_ratio[k][i] <= low_point:
        #             wgtsDict[k][i] = - int(3. * wgt_daily[k])
        #         elif basis_spread_ratio[k][i] >= high_point:
        #             wgtsDict[k][i] = int(3. * wgt_daily[k])

        wgtsDict = self.wgtsProcess(wgtsDict)

        self.displayResult(wgtsDict, saveLocal=True)


if __name__ == '__main__':
    a = MA_ATR()
    a.strategy()


