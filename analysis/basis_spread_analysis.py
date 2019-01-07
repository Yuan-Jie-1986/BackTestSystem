# coding=utf-8

from lib.simulator.base import BacktestSys
import re
import matplotlib.pyplot as plt
import numpy as np

class BasisSpreadAnalysis(BacktestSys):
    def __init__(self):
        super(BasisSpreadAnalysis, self).__init__()

    def analysis(self):
        cmd_dict = {}
        for k in self.category:
            cmd_dict.setdefault(self.category[k], []).append(k)
        cmd = 'TA'
        ptn = re.compile('_MC_LastMonthEnd')
        for nm in cmd_dict[cmd]:
            if ptn.search(nm):
                futures = self.data[nm]['CLOSE']
                remains = self.data[nm]['remain_days']
            else:
                spot = self.data[nm].values()[0]

        basis = spot - futures

        ## 分析剩余天数对收益的影响情况
        remains_next = np.ones_like(remains) * np.nan
        remains_next[:-1] = remains[1:]
        con_switch = remains > remains_next

        print self.dt[~con_switch]





        # ## 绘图
        # plt.subplot(311)
        # plt.title('The Basis Spread Analysis of %s' % cmd)
        # plt.plot_date(self.dt, futures, fmt='-r', label='Futures')
        # plt.plot_date(self.dt, spot, fmt='-k', label='Spot')
        # plt.grid()
        # plt.legend(loc='best')
        # plt.subplot(312)
        # plt.plot_date(self.dt, basis, fmt='-g', label='BasisSpread')
        # plt.grid()
        # plt.legend(loc='best')
        # plt.subplot(313)
        # plt.plot_date(self.dt, remains, fmt='-r', label='Remain Days')
        # plt.grid()
        # plt.legend(loc='best')
        #
        # plt.show()


if __name__ == '__main__':
    a = BasisSpreadAnalysis()
    a.analysis()