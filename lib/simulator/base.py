# coding=utf-8

import numpy as np
import pandas as pd
import pymongo
import matplotlib.pyplot as plt
from datetime import datetime, timedelta


class TradeRecord(object):

    def __init__(self):
        self.open = None
        self.open_dt = None
        self.close = None
        self.close_dt = None
        self.volume = None
        self.direction = None
        self.contract = ''
        self.commodity = ''
        self.multiplier = None
        self.count = None
        self.pnl = None


    def calcPnL(self):
        self.pnl = (self.close - self.open) * self.volume * self.multiplier * self.direction

    def setOpen(self, val):
        self.open = val

    def setOpenDT(self, val):
        self.open_dt = val

    def setClose(self, val):
        self.close = val

    def setCloseDT(self, val):
        self.close_dt = val

    def setVolume(self, val):
        self.volume = val

    def setDirection(self, val):
        self.direction = val

    def setContract(self, val):
        self.contract = val

    def setCommodity(self, val):
        self.commodity = val

    def setMultiplier(self, val):
        self.multiplier = val

    def setCounter(self, val):
        self.count = val


class BacktestSys(object):

    def __init__(self):

        self.start_date = None # 策略起始日期
        self.end_date = None # 结束日期
        # self.lookback = None
        # self.init_date = None
        self.net_value = None # 净值曲线
        self.periods = None # 持仓时间长度
        self.rtn_daily = None # 日收益率
        # self.weights = None  # 日权重
        self.leverage = None  # 杠杆率
        self.capital = None  # 总资金
        self.bt_mode = None
        self.contract = None  # 交易的合约
        # self.multiplier = None  # 合约乘数
        self.margin_ratio = None

        # 连接mongoDB数据库
        self.conn = pymongo.MongoClient(host='192.168.2.171', port=27017)

    def useDBCollections(self, db, collection):
        dbs = self.conn[db]
        col = dbs[collection]
        return col

    def strategy(self):
        raise NotImplementedError

    def calc_pnl(self, db, collection, contract, multiplier, wgts):
        """
        根据生成的权重计算盈亏情况，需要的数据，db是数据库，collection是数据表，contract是合约，multiplier是合约乘数。
        然后根据wgts权重向量计算。需要注意的问题是权重向量与时间向量的长度要相同。
        """

        if self.bt_mode == 'NextOpen':
            weights = np.zeros(wgts.shape)
            weights[1:] = wgts[:-1]
            res = self.prepareData(db=db, collection=collection, contract=contract, field=['date', 'OPEN', 'CLOSE'])
            dt = res['date']
            open_price = res['OPEN']
            close_price = res['CLOSE']

            if dt.shape != wgts.shape:
                raise Exception('日期向量与权重向量的长度不一致')

            pnl = np.zeros_like(wgts)

            for i in range(1, len(weights)):

                # 当天增减仓
                if weights[i] * weights[i-1] >= 0:
                    delta_wgt = weights[i] - weights[i-1]
                    pnl[i] = pnl[i-1] + (close_price[i] - open_price[i]) * delta_wgt * multiplier + \
                             (close_price[i] - close_price[i-1]) * weights[i-1] * multiplier
                # 当天改变持仓方向
                elif weights[i] * weights[i-1] < 0:
                    pnl[i] = pnl[i-1] + (close_price[i] - open_price[i]) * weights[i] * multiplier + \
                             (open_price[i] - close_price[i-1]) * weights[i-1] * multiplier

        pnl_df = pd.DataFrame({'pnl': pnl}, index=dt)
        return pnl_df

    def stat_trade(self, db, collection, contract, multiplier, wgts):
        """
        对每笔交易进行统计，需要的数据，db是数据库，collection是数据表，contract是合约，multiplier是合约乘数，wgts是交易的手数。
        """

        count = 0
        if self.bt_mode == 'NextOpen':
            weights = np.zeros(wgts.shape)
            weights[1:] = wgts[:-1]
            res = self.prepareData(db=db, collection=collection, contract=contract, field=['date', 'OPEN', 'CLOSE'])
            dt = res['date']
            open_price = res['OPEN']
            close_price = res['CLOSE']

            if dt.shape != wgts.shape:
                raise Exception('日期向量与权重向量的长度不一致')

            trade_record = []
            unclosed = 0

            for i in range(1, len(weights)):

                if abs(weights[i]) > abs(weights[i-1]) and weights[i] * weights[i-1] >= 0:

                    # 新开仓
                    count += 1
                    tr_r = TradeRecord()
                    tr_r.setCounter(count)
                    tr_r.setOpen(open_price[i])
                    tr_r.setOpenDT(dt[i])
                    tr_r.setVolume(abs(weights[i]) - abs(weights[i-1]))
                    tr_r.setMultiplier(multiplier)
                    tr_r.setContract(contract)
                    tr_r.setDirection(np.sign(weights[i]))
                    trade_record.append(tr_r)
                    unclosed += tr_r.volume
                    unclosed_dir = tr_r.direction

                elif abs(weights[i]) < abs(weights[i-1]) and weights[i] * weights[i-1] >= 0:

                    # 减仓或平仓
                    j = 1
                    while unclosed != 0 and - unclosed_dir == np.sign(weights[i] - weights[i-1]):
                        trade_record[-j].setClose(open_price[i])
                        trade_record[-j].setCloseDT(dt[i])
                        trade_record[-j].calcPnL()
                        unclosed -= trade_record[-j].volume
                        if unclosed == 0:
                            unclosed_dir = None
                        j += 1

                elif weights[i] * weights[i-1] < 0:

                    # 平仓后反方向开仓
                    j = 1
                    while unclosed != 0 and unclosed_dir == np.sign(weights[i-1]):
                        trade_record[-j].setClose(open_price[i])
                        trade_record[-j].setCloseDT(dt[i])
                        trade_record[-j].calcPnL()
                        unclosed -= trade_record[-j].volume
                        if unclosed == 0:
                            unclosed_dir = None
                        j += 1

                    count += 1
                    tr_r = TradeRecord()
                    tr_r.setCounter(count)
                    tr_r.setOpen(open_price[i])
                    tr_r.setOpenDT(dt[i])
                    tr_r.setVolume(abs(weights[i]))
                    tr_r.setMultiplier(multiplier)
                    tr_r.setContract(contract)
                    tr_r.setDirection(np.sign(weights[i]))
                    trade_record.append(tr_r)
                    unclosed += tr_r.volume
                    unclosed_dir = tr_r.direction

        trade_times = len(trade_record)
        buy_times = len([t for t in trade_record if t.direction == 1])
        sell_times = len([t for t in trade_record if t.direction == -1])
        profit_times = len([t for t in trade_record if t.pnl > 0])
        loss_times = len([t for t in trade_record if t.pnl < 0])
        buy_pnl = [t.pnl for t in trade_record if t.direction == 1]
        sell_pnl = [t.pnl for t in trade_record if t.direction == -1]
        buy_avg_pnl = sum(buy_pnl) / buy_times
        sell_avg_pnl = sum(sell_pnl) / sell_times

        print '+++++++++++++++%s合约交易统计++++++++++++++++++++' % contract
        print '交易次数: %d' % trade_times
        print '做多次数: %d' % buy_times
        print '做空次数: %d' % sell_times
        print '盈利次数: %d' % profit_times
        print '亏损次数: %d' % loss_times
        print '做多平均盈亏: %f' % buy_avg_pnl
        print '做空平均盈亏: %f' % sell_avg_pnl

        return trade_record



    def prepareData(self, db, collection, contract, field=['date', 'CLOSE']):

        col = self.useDBCollections(db=db, collection=collection)

        if not self.start_date:
            self.start_date = datetime.strptime('20000101', '%Y%m%d')
        if not self.end_date:
            self.end_date = datetime.today()
        # if not self.lookback:
        #     self.lookback = 0

        if isinstance(self.start_date, str):
            self.start_date = datetime.strptime(self.start_date, '%Y%m%d')
        if isinstance(self.end_date, str):
            self.end_date = datetime.strptime(self.end_date, '%Y%m%d')

        # queryArgs = {'wind_code': contract}
        # projectionField = ['date']
        #
        # res = col.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING).limit(1)
        #
        # contract_init = list(res)[0]['date']

        # if self.start_date < contract_init:
        #     self.init_date = contract_init
        # elif self.lookback == 0:
        #     self.init_date = self.start_date
        # else:
        #     queryArgs = {'wind_code': contract,
        #                  'date': {'$lt': self.start_date}}
        #     projectionField = ['date']
        #     res = col.find(queryArgs, projectionField).sort('date', pymongo.DESCENDING).limit(self.lookback)
        #     self.init_date = list(res)[-1]['date']

        queryArgs = {'wind_code': contract,
                     'date': {'$gte': self.start_date, '$lte': self.end_date}}
        projectionField = field
        res = col.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
        preData = dict()

        for f in projectionField:
            preData[f] = []

        for r in res:
            for f in projectionField:
                preData[f].append(r[f])

        for f in projectionField:
            preData[f] = np.array(preData[f])
        return preData

    def calcBTResult(self):
        self.calcRtn()
        print '年化收益率：', self.calcAnnualRTN()
        print '夏普比率：', self.calcSharpe()
        print '最大回撤：', self.calcMaxDrawdown()

    def calcRtn(self):
        self.rtn_daily = np.ones(len(self.net_value)) * np.nan
        self.rtn_daily[1:] = self.net_value[1:] / self.net_value[:-1] - 1.

    def calcAnnualRTN(self):

        return np.nanmean(self.rtn_daily) * 250.

    def calcAnnualSTD(self):
        return np.nanstd(self.rtn_daily) * np.sqrt(250)

    def calcSharpe(self):
        return self.calcAnnualRTN() / self.calcAnnualSTD()

    def calcMaxDrawdown(self):

        index_end = np.argmax(np.maximum.accumulate(self.net_value) - self.net_value)
        index_start = np.argmax(self.net_value[:index_end])
        max_drawdown = self.net_value[index_end] - self.net_value[index_start]
        return max_drawdown

if __name__ == '__main__':
    a = BacktestSys()
    # print a.net_value
    # print a.calcSharpe()
    # print a.calcMaxDrawdown()
    # a.generateNV()
    # print a.calcBTResult()
    # plt.plot(a.net_value)
    # plt.show()
    a.start_date = '20180101'
    a.prepareData(db='FuturesDailyWind', collection='TA.CZC_Daily', contract='TA.CZC')
