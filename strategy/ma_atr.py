# coding=utf-8

from lib.simulator.base import BacktestSys
import numpy as np
import re
import pandas as pd

class MA_ATR(BacktestSys):
    def __init__(self):
        # super(MA_ATR, self).__init__()
        self.current_file = __file__
        self.prepare()

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

        return wgtsDict

if __name__ == '__main__':
    a = MA_ATR()
    wgtsDict = a.strategy()
    wgtsDict = a.wgtsProcess(wgtsDict)
    a.displayResult(wgtsDict, saveLocal=True)


