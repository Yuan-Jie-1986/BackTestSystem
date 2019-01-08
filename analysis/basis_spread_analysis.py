# coding=utf-8

from lib.simulator.base import BacktestSys
import re
import matplotlib.pyplot as plt
import numpy as np
import pprint
import pandas as pd

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
        spot = pd.DataFrame(spot).fillna(method='ffill').values.flatten()

        basis = spot - futures
        ## 分析剩余天数对收益的影响情况
        times = 0
        pre_days = 30
        res_dict = {}
        open_flag = False
        for i in np.arange(len(self.dt)):

            if np.isnan(basis[i]) or np.isnan(spot[i]) or np.isnan(futures[i]):
                continue

            # if i == 0 or remains[i] > remains[i-1]:
            if remains[i] == pre_days or (remains[i-1] > pre_days > remains[i]):
                times += 1

                res_dict[times] = {'basis_open': basis[i],
                                   'fut_open': futures[i],
                                   'dt_open': self.dt[i],
                                   'remain_open': remains[i],
                                   'direction': 1 if basis[i] > 0 else -1}
                open_flag = True


            elif open_flag and (i == len(self.dt) - 1 or remains[i] < remains[i + 1]):
                if times not in res_dict or 'dt_close' in res_dict[times]:
                    print times, self.dt[i], res_dict[times]
                    raise Exception(u'没有之前的开仓数据')
                res_dict[times]['basis_close'] = basis[i]
                res_dict[times]['fut_close'] = futures[i]
                res_dict[times]['dt_close'] = self.dt[i]
                res_dict[times]['remain_close'] = remains[i]
                res_dict[times]['basis_rtn'] = (res_dict[times]['basis_close'] - res_dict[times]['basis_open']) * \
                                               (-res_dict[times]['direction'])
                res_dict[times]['fut_rtn'] = (res_dict[times]['fut_close'] - res_dict[times]['fut_open']) * \
                                               res_dict[times]['direction']
                open_flag = False

        basis_rtn = []
        fut_rtn = []
        times = []
        for d in res_dict:
            times.append(d)
            basis_rtn.append(res_dict[d]['basis_rtn'])
            fut_rtn.append(res_dict[d]['fut_rtn'])
        times = np.array(times)
        basis_rtn = np.array(basis_rtn)
        fut_rtn = np.array(fut_rtn)

        total = len(times)
        BasisCorr = len(basis_rtn[basis_rtn > 0.])
        BasisPct = BasisCorr * 1. / len(basis_rtn)
        BasisRtn = np.mean(basis_rtn)
        FutCorr = len(fut_rtn[fut_rtn > 0.])
        FutPct = FutCorr * 1. / len(fut_rtn)
        FutRtn = np.mean(fut_rtn)

        print u'总次数：', total
        print u'Basis准确率：', BasisPct
        print u'Basis准确次数：', BasisCorr
        print u'Basis平均盈利：', BasisRtn
        print u'Futures准确率：', FutPct
        print u'Futures准确次数：', FutCorr
        print u'Futures平均盈利：', FutRtn


        width = 0.4
        plt.bar(times, basis_rtn, width, color='r', label='BasisRtn')
        plt.bar(times+width, fut_rtn, width, color='k', label='FutRtn')
        plt.legend()
        plt.grid()
        plt.title('%s yield' % cmd)
        plt.show()
        pprint.pprint(res_dict)


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