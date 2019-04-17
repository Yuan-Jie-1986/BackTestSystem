# coding=utf-8

from lib.simulator.base import BacktestSys
import numpy as np
import re
from basis_spread_chemical import BasisSpreadChemical
from basis_spread_ferrous import BasisSpreadFerrous

class BasisSpread(BacktestSys):
    def __init__(self):
        # super(BasisSpread, self).__init__()
        self.current_file = __file__
        self.prepare()

    def strategy(self):
        a = BasisSpreadChemical()
        wgtsDict = a.strategy()
        wgtsDict = a.wgtsStandardization(wgtsDict)
        # wgtsDict = a.wgtsProcess(wgtsDict)

        # b = BasisSpreadChemical()
        # wgts_b = b.strategy()
        # wgts_b = b.wgtsStandardization(wgts_b)
        # # wgts_b = b.wgtsProcess(wgts_b)
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
        #             final_wgt[k] = wgts_b[k]

        return wgtsDict




if __name__ == '__main__':


    a = BasisSpread()
    wgtsDict = a.strategy()
    wgtsDict = a.wgtsStandardization(wgtsDict)
    wgtsDict = a.wgtsProcess(wgtsDict)
    a.displayResult(wgtsDict, saveLocal=True)


