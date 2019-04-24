# coding=utf-8

from lib.simulator.base import BacktestSys
import numpy as np
import re
import pandas as pd


class BasisSpread(BacktestSys):
    def __init__(self):
        # super(BasisSpread, self).__init__()
        self.current_file = __file__
        self.prepare()

    def strategy(self):

        # # 生成一个字典，对于每个品种的，期货和现货的名字
        # pairs_dict = {}
        # for k, v in self.category.iteritems():
        #     if v not in pairs_dict:
        #         pairs_dict[v] = [k]
        #     else:
        #         pairs_dict[v].append(k)
        #
        # basis_spread = {}
        # basis_spread_ratio = {}
        # basis_spread_tsrank = {}
        # wgtsDict = {}
        # patten = re.compile('(?<=\w)\.(?=[A-Z]+)')  # 用来判断是否为期货
        # for k, v in pairs_dict.items():
        #     for sub_v in v:
        #         if patten.search(sub_v):
        #             futures_contract = sub_v
        #             futures_price = self.data[sub_v]['CLOSE']
        #         else:
        #             spot_price = self.data[sub_v].values()[0]
        #             spot_price_new = np.ones_like(spot_price) * np.nan
        #             spot_price_new[1:] = spot_price[1:]
        #
        #     if "futures_contract" not in locals():
        #         raise Exception(u'没有期货合约')
        #
        #     basis_spread[futures_contract] = spot_price_new - futures_price
        #     basis_spread_ratio[futures_contract] = 1 - futures_price / spot_price_new
        #     temp = spot_price_new - futures_price
        #     temp_high = pd.DataFrame(temp).rolling(window=60, min_periods=55).max().values.flatten()
        #     temp_low = pd.DataFrame(temp).rolling(window=60, min_periods=55).min().values.flatten()
        #     temp_ratio = (temp - temp_low) / (temp_high - temp_low)
        #     # basis_spread_ratio[futures_contract] = temp_ratio
        #     basis_spread_ratio[futures_contract] = basis_spread_ratio[futures_contract] * temp_ratio
        #
        #     wgtsDict[futures_contract] = np.zeros_like(self.dt)
        #
        #     del futures_contract
        #
        # for i in np.arange(len(self.dt)):
        #
        #     # 根据基差比例进行交易，多正基差最大的n只，空负基差最小的n只
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
        #     for k in basis_spread_ratio:
        #         if basis_spread_ratio[k][i] <= low_point:
        #             wgtsDict[k][i] = -1  #- int(5. * wgt_daily[k])
        #         elif basis_spread_ratio[k][i] >= high_point:
        #             wgtsDict[k][i] = 1  # int(5. * wgt_daily[k])

        wgtsDict = {}
        vol2oi = {}
        rtn5 = {}
        for k in self.data:
            wgtsDict[k] = np.zeros(len(self.dt))
            vol2oi[k] = self.data[k]['VOLUME'] / self.data[k]['OI']
            rtn5[k] = np.ones(len(self.dt)) * np.nan
            rtn5[k][5:] = self.data[k]['CLOSE'][5:] / self.data[k]['CLOSE'][:-5] - 1.

        for i in np.arange(len(self.dt)):
            vol2oi_daily = []
            rtn5_daily = []
            for k in vol2oi:
                vol2oi_daily.append(vol2oi[k][i])
                rtn5_daily.append(rtn5[k][i])
            vol2oi_daily = np.array(vol2oi_daily)
            rtn5_daily = np.array(rtn5_daily)
            if len(vol2oi_daily[~np.isnan(vol2oi_daily)]) <= 1 or len(rtn5_daily[~np.isnan(rtn5_daily)]) <= 1:
                continue
            count = min(len(vol2oi_daily[~np.isnan(vol2oi_daily)]), len(rtn5_daily[~np.isnan(rtn5_daily)]))
            rtn5_series = rtn5_daily[~np.isnan(rtn5_daily)] * vol2oi_daily[~np.isnan(rtn5_daily)]
            rtn5_series.sort()
            num_selection = min(3, count / 2)
            low_point = rtn5_series[num_selection-1]
            high_point = rtn5_series[-num_selection]
            for k in rtn5:
                if rtn5[k][i] * vol2oi[k][i] <= low_point:
                    wgtsDict[k][i] = -1  #- int(5. * wgt_daily[k])
                elif rtn5[k][i] * vol2oi[k][i] >= high_point:
                    wgtsDict[k][i] = 1  # int(5. * wgt_daily[k])


        return wgtsDict


if __name__ == '__main__':
    a = BasisSpread()
    wgtsDict = a.strategy()
    wgtsDict = a.wgtsStandardization(wgtsDict)
    wgtsDict = a.wgtsProcess(wgtsDict)
    # for k in wgtsDict:
    #     wgtsDict[k] = 2 * np.array(wgtsDict[k])

    a.displayResult(wgtsDict, saveLocal=True)


