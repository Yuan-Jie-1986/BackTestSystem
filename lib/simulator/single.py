# coding=utf-8

from base import BacktestSys
import numpy as np
import pandas as pd
import pymongo
import matplotlib.pyplot as plt
import matplotlib.dates as mdt
from collections import  OrderedDict

import warnings

warnings.filterwarnings('ignore')


class SingleBT(BacktestSys):

    def __init__(self):
        super(SingleBT, self).__init__()

    def strategy(self):

        cls = self.data['TA809.CZC']['CLOSE']
        ma20 = pd.DataFrame(cls).rolling(window=20).mean().values.flatten()
        ma10 = pd.DataFrame(cls).rolling(window=10).mean().values.flatten()
        con = np.zeros(cls.shape)
        con[cls > ma20] = 1
        con[(cls < ma10) * (cls > ma20)] = 0
        con[cls < ma20] = -1
        con[(cls > ma10) * (cls < ma20)] = 0
        wgtDict = {'TA809.CZC': con}

        # 该步骤一样要有
        wgtDict = self.wgtsProcess(wgtDict)

        self.displayResult(wgtDict, True)







    # def generateNV(self):
    #
    #     dt, con = self.strategy()
    #
    #     count = 0
    #     if self.bt_mode == 'NextOpen':
    #         self.weights = np.zeros(con.shape)
    #         self.weights[1:] = con[:-1]
    #         res = self.prepareData(db='FuturesDailyWind', collection='TA.CZC_Daily', contract='TA809.CZC', field=['date', 'OPEN', 'CLOSE'])
    #         open_price = res['OPEN']
    #         close_price = res['CLOSE']
    #         self.net_value = np.ones(con.shape) * self.capital
    #
    #         trade_record = []
    #         leverage = np.zeros(con.shape)
    #         margin_usage = np.zeros(con.shape)
    #         unclosed = 0
    #
    #         for i in range(1, len(self.weights)):
    #
    #             # 当天增减仓
    #             if self.weights[i] * self.weights[i-1] >= 0:
    #                 delta_weight = self.weights[i] - self.weights[i-1]
    #                 self.net_value[i] = self.net_value[i-1] + (close_price[i] - open_price[i]) * delta_weight * \
    #                                     self.multiplier + (close_price[i] - close_price[i-1]) * self.weights[i-1] * self.multiplier
    #             # 当天改变持仓方向
    #             elif self.weights[i] * self.weights[i-1] < 0:
    #                 self.net_value[i] = self.net_value[i-1] + (close_price[i] - open_price[i]) * self.weights[i] * \
    #                                     self.multiplier + (open_price[i] - close_price[i-1]) * self.weights[i-1] * self.multiplier
    #
    #             leverage[i] = abs(self.weights[i]) * self.multiplier * close_price[i] / self.net_value[i]
    #             margin_usage[i] = abs(self.weights[i]) * self.multiplier * close_price[i] * self.margin_ratio / self.net_value[i]
    #
    #             if abs(self.weights[i]) > abs(self.weights[i-1]) and self.weights[i] * self.weights[i-1] >= 0:
    #
    #                 # 新开仓
    #
    #                 count += 1
    #                 tr_r = TradeRecord()
    #                 tr_r.setCounter(count)
    #                 tr_r.setOpen(open_price[i])
    #                 tr_r.setOpenDT(dt[i])
    #                 tr_r.setVolume(abs(self.weights[i]) - abs(self.weights[i-1]))
    #                 tr_r.setMultiplier(self.multiplier)
    #                 tr_r.setContract(self.contract)
    #                 tr_r.setDirection(np.sign(self.weights[i]))
    #                 trade_record.append(tr_r)
    #                 unclosed = unclosed + tr_r.volume
    #                 unclosed_dir = tr_r.direction
    #
    #                 # open_position.append(self.weights[i] * open_price[i] * self.multiplier)
    #
    #             elif abs(self.weights[i]) < abs(self.weights[i-1]) and self.weights[i] * self.weights[i-1] >= 0:
    #
    #                 j = 1
    #                 while unclosed != 0 and - unclosed_dir == np.sign(self.weights[i] - self.weights[i-1]):
    #                     trade_record[-j].setClose(open_price[i])
    #                     trade_record[-j].setCloseDT(dt[i])
    #                     trade_record[-j].calcPnL()
    #                     unclosed -= trade_record[-j].volume
    #                     if unclosed == 0:
    #                         unclosed_dir = None
    #                     j += 1
    #
    #
    #             elif self.weights[i] * self.weights[i-1] < 0:
    #                 # 平仓后反方向开仓
    #                 j = 1
    #                 while unclosed != 0 and unclosed_dir == np.sign(self.weights[i-1]):
    #                     trade_record[-j].setClose(open_price[i])
    #                     trade_record[-j].setCloseDT(dt[i])
    #                     trade_record[-j].calcPnL()
    #                     unclosed -= trade_record[-j].volume
    #                     if unclosed == 0:
    #                         unclosed_dir = None
    #                     j += 1
    #                 count += 1
    #                 tr_r = TradeRecord()
    #                 tr_r.setCounter(count)
    #                 tr_r.setOpen(open_price[i])
    #                 tr_r.setOpenDT(dt[i])
    #                 tr_r.setVolume(abs(self.weights[i]))
    #                 tr_r.setMultiplier(self.multiplier)
    #                 tr_r.setContract(self.contract)
    #                 tr_r.setDirection(np.sign(self.weights[i]))
    #                 trade_record.append(tr_r)
    #                 unclosed = unclosed + tr_r.volume
    #                 unclosed_dir = tr_r.direction
    #
    #         trade_times = len(trade_record)
    #         buy_times = len([t for t in trade_record if t.direction == 1])
    #         sell_times = len([t for t in trade_record if t.direction == -1])
    #         profit_times = len([t for t in trade_record if t.pnl > 0])
    #         loss_times = len([t for t in trade_record if t.pnl < 0])
    #         pnl_list = [t.pnl for t in trade_record]
    #         buy_pnl = [t.pnl for t in trade_record if t.direction == 1]
    #         sell_pnl = [t.pnl for t in trade_record if t.direction == -1]
    #
    #         buy_avg_pnl = sum(buy_pnl) / buy_times
    #         sell_avg_pnl = sum(sell_pnl) / sell_times
    #
    #         print '\n+++++++++++++++回测结果++++++++++++++++++\n'
    #         print '交易次数: %d' % trade_times
    #         print '做多次数: %d' % buy_times
    #         print '做空次数: %d' % sell_times
    #         print '盈利次数: %d' % profit_times
    #         print '亏损次数: %d' % loss_times
    #         print '做多平均盈亏: %f' % buy_avg_pnl
    #         print '做空平均盈亏: %f' % sell_avg_pnl
    #
    #         plt.subplot(211)
    #         plt.plot_date(dt, self.net_value, fmt='-r', label='MarketValue')
    #         plt.grid()
    #         plt.legend()
    #
    #         plt.subplot(212)
    #         plt.hist(pnl_list, bins=100, color='r', label='DistributionOfPNL')
    #         plt.legend()
    #
    #
    #     self.calcBTResult()
    #
    #     plt.show()




    def test(self):
        print locals()



if __name__ == '__main__':
    a = SingleBT()
    # print a.useDBCollections(db='FuturesDailyWind', collection='TA.CZC_Daily')
    # b = a.prepareData(db='FuturesDailyWind', collection='TA.CZC_Daily', contract='TA.CZC')
    # b = a.prepareData(db='EDBWind', collection='FX', contract='M0067855')
    # _, con = a.strategy()

    a.strategy()

    # tr = a.stat_trade(db='FuturesDailyWind', collection='TA.CZC_Daily', contract='TA809.CZC', multiplier=5, wgts=con)
    # print tr
    # for t in tr:
    #     print t.__dict__

    # a.generateNV()
    # print b

