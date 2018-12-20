# coding=utf-8

from lib.simulator.base import BacktestSys
import numpy as np
import re

class BasisSpread(BacktestSys):
    def __init__(self):
        self.current_file = __file__
        self.prepare()

    def strategy(self):

        # 生成一个字典，对于每个品种的，期货和现货的名字
        pairs_dict = {}
        for k, v in self.category.iteritems():
            if v not in pairs_dict:
                pairs_dict[v] = [k]
            else:
                pairs_dict[v].append(k)

        basis_spread = {}
        basis_spread_ratio = {}
        wgtsDict = {}
        patten = re.compile('(?<=\w)\.(?=[A-Z]+)')  # 用来判断是否为期货
        for k, v in pairs_dict.items():
            for sub_v in v:
                if patten.search(sub_v):
                    futures_contract = sub_v
                    futures_price = self.data[sub_v]['CLOSE']
                else:
                    spot_price = self.data[sub_v].values()[0]
                    spot_price_new = np.ones_like(spot_price) * np.nan
                    spot_price_new[1:] = spot_price[:-1]

            if "futures_contract" not in locals():
                raise Exception(u'没有期货合约')

            basis_spread[futures_contract] = spot_price_new - futures_price
            basis_spread_ratio[futures_contract] = 1 - futures_price / spot_price_new
            wgtsDict[futures_contract] = np.zeros_like(self.dt)

            del futures_contract

        for i in np.arange(len(self.dt)):
            bsr_daily = []
            for k in basis_spread_ratio:
                bsr_daily.append(basis_spread_ratio[k][i])
            bsr_daily = np.array(bsr_daily)
            count = len(bsr_daily[~np.isnan(bsr_daily)])
            if count <= 1:
                continue
            bsr_series = bsr_daily[~np.isnan(bsr_daily)]
            bsr_series.sort()
            num_selection = min(3, count / 2)
            low_point = bsr_series[num_selection-1]
            high_point = bsr_series[-num_selection]

            for k in basis_spread_ratio:
                if basis_spread_ratio[k][i] <= low_point:
                    wgtsDict[k][i] = -1.
                elif basis_spread_ratio[k][i] >= high_point:
                    wgtsDict[k][i] = 1.

        self.statsTotal(wgtsDict)

a = BasisSpread()
a.strategy()


