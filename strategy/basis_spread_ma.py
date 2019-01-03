# coding=utf-8

from lib.simulator.base import BacktestSys
from basis_spread import BasisSpread
from ma_atr import MA_ATR
import numpy as np



if __name__ == '__main__':
    a = BasisSpread()
    a_wgt = a.strategy()
    b = MA_ATR()
    b_wgt = b.strategy()

    con1 = np.in1d(a.dt, b.dt)
    con2 = np.in1d(b.dt, a.dt)
    if len(a.dt) == len(b.dt) and con1.all() and con2.all():
        final_wgt = {}
        for k in a_wgt:
            final_wgt[k] = a_wgt[k] + b_wgt[k]

    c = BacktestSys()
    final_wgt = c.wgtsProcess(final_wgt)
    final_wgt = c.wgtsStandardization(final_wgt)
    c.displayResult(wgtsDict=final_wgt, saveLocal=True)




