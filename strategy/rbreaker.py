# coding=utf-8

from lib.simulator.base import BacktestSys
import numpy as np
import pandas as pd

class RBreaker(BacktestSys):
    def __init__(self):
        super(RBreaker, self).__init__()

    def strategy(self):

        wgtDict = {}
        for k in self.data:
            wgtDict[k] = np.zeros_like(self.dt)
            high = pd.DataFrame(self.data[k]['HIGH']).rolling(window=120, min_periods=100).mean().values.flatten()
            low = pd.DataFrame(self.data[k]['LOW']).rolling(window=120, min_periods=100).mean().values.flatten()
            cls = pd.DataFrame(self.data[k]['CLOSE']).rolling(window=120, min_periods=100).mean().values.flatten()
            pivot = (high + low + cls) / 3.
            bbreak = high + 2 * (pivot - low)
            ssetup = pivot + high - low
            senter = 2 * pivot - low
            benter = 2 * pivot - high
            bsetup = pivot - high + low
            sbreak = low - 2 * (high - pivot)

            for i in np.arange(1, len(self.dt)):
                if wgtDict[k][i-1] == 0:
                    if self.data[k]['CLOSE'][i] > bbreak[i]:
                        wgtDict[k][i] = 1.
                    if self.data[k]['CLOSE'][i] < sbreak[i]:
                        if wgtDict[k][i] == 1:
                            print self.dt[i], k, bbreak[i], sbreak[i]
                            raise Exception(u'出错')
                        wgtDict[k][i] = -1.
                elif wgtDict[k][i-1] == 1:
                    if self.data[k]['HIGH'][i] > ssetup[i] and self.data[k]['CLOSE'][i] < senter[i]:
                        wgtDict[k][i] = -1.
                elif wgtDict[k][i-1] == -1:
                    if self.data[k]['LOW'][i] < bsetup[i] and self.data[k]['CLOSE'][i] > benter[i]:
                        wgtDict[k][i] = 1.
                else:
                    wgtDict[k][i] = wgtDict[k][i-1]

        return wgtDict


if __name__ == '__main__':
    a = RBreaker()
    wgts = a.strategy()
    wgts = a.wgtsStandardization(wgts)
    wgts = a.wgtsProcess(wgts)

    a.displayResult(wgts)
