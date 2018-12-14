# coding=utf-8

import numpy as np
import pandas as pd
import pymongo
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import pprint
import yaml
import os


# 单次的交易记录
class TradeRecordByTimes(object):
    def __init__(self):
        self.dt = None  # 交易时间
        self.trade_commodity = None  # 交易商品
        self.trade_contract = None  # 交易合约
        self.trade_direction = None  # 交易方向, 1为做多，-1为做空
        self.trade_price = None  # 交易价格
        self.trade_volume = None  # 交易量
        self.trade_multiplier = None  # 交易乘数
        self.trade_margin_ratio = None  # 保证金比率
        self.trade_margin_occupation = None  # 保证金占用
        self.trade_type = None  # 交易类型，是平仓还是开仓。1为开仓，-1为平仓

    def setDT(self, val):
        self.dt = val

    def setPrice(self, val):
        self.trade_price = val

    def setCommodity(self, val):
        self.trade_commodity = val

    def setContract(self, val):
        self.trade_contract = val

    def setDirection(self, val):
        self.trade_direction = val

    def setType(self, val):
        self.trade_type = val

    def setVolume(self, val):
        self.trade_volume = val

    def setMultiplier(self, val):
        self.trade_multiplier = val

    def setMarginRatio(self, val):
        self.trade_margin_ratio = val

    def calMarginOccupation(self):
        self.trade_margin_occupation = self.trade_price * self.trade_multiplier * self.trade_margin_ratio * \
                                       self.trade_volume

# 逐笔的交易记录
class TradeRecordByTrade(object):

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
        self.rtn = None
        self.holding_period = None  # 该交易的交易周期

    def calcPnL(self):
        self.pnl = (self.close - self.open) * self.volume * self.multiplier * self.direction

    def calcRtn(self):
        self.rtn = self.direction * ((self.close / self.open) - 1.)

    def calcHoldingPeriod(self):
        self.holding_period = (self.close_dt - self.open_dt + timedelta(1)).days

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


# 逐日的交易记录
class TradeRecordByDay(object):

    def __init__(self, dt, holdPosDict, MkData, newTrade):
        self.dt = dt  # 日期
        self.newTrade = newTrade  # 当天进行的交易
        self.mkdata = MkData  # 合约市场数据
        self.holdPosition = holdPosDict  # 之前已有的持仓, 字典中的volume的值是有正有负，正值代表持多仓，负值为空仓
        self.daily_pnl = 0

    def addNewPositon(self):

        for tObj in self.newTrade:

            if tObj.dt != self.dt:
                continue

            if tObj.trade_contract not in self.holdPosition:
                self.holdPosition[tObj.trade_contract] = dict()

            self.holdPosition[tObj.trade_contract].setdefault('newTrade', []).append(tObj)

    def getFinalMK(self):

        for k, v in self.holdPosition.items():

            if 'volume' in v:

                if 'PRECLOSE' not in self.mkdata[k]:
                    raise Exception(u'请检查传入的市场数据是否正确')

                self.daily_pnl = self.daily_pnl + v['volume'] * (self.mkdata[k]['CLOSE'] - self.mkdata[k]['PRECLOSE']) \
                                 * self.mkdata[k]['multiplier']
            else:
                self.holdPosition[k]['volume'] = 0

            if 'newTrade' in v:

                for nt in v['newTrade']:

                    self.daily_pnl = self.daily_pnl + nt.trade_volume * nt.trade_direction * nt.trade_multiplier * \
                                     (self.mkdata[k]['CLOSE'] - nt.trade_price)

                    self.holdPosition[k]['volume'] = self.holdPosition[k]['volume'] + nt.trade_volume * \
                                                     nt.trade_direction

                del self.holdPosition[k]['newTrade']

            if self.holdPosition[k]['volume'] == 0:
                del self.holdPosition[k]

        return self.daily_pnl

    def getHoldPosition(self):
        return self.holdPosition


class BacktestSys(object):

    def __init__(self):
        # self.start_date = None  # 策略起始日期
        # self.end_date = None  # 结束日期
        # # self.lookback = None
        # # self.init_date = None
        # self.net_value = None  # 净值曲线
        # self.periods = None  # 持仓时间长度
        # self.rtn_daily = None  # 日收益率
        # # self.weights = None  # 日权重
        # self.leverage = None  # 杠杆率
        # self.capital = None  # 总资金
        # self.bt_mode = None
        # self.contract = None  # 交易的合约
        # # self.multiplier = None  # 合约乘数
        # self.margin_ratio = None
        pass

    def prepare(self):
        # 根据yaml配置文件从数据库中抓取数据，并对回测进行参数设置
        current_yaml = '.'.join((os.path.splitext(self.current_file)[0], 'yaml'))
        f = open(current_yaml)
        conf = yaml.load(f)

        # 回测起始时间
        self.start_dt = datetime.strptime(conf['start_date'], '%Y%m%d')

        # 回测结束时间
        self.end_dt = datetime.strptime(conf['end_date'], '%Y%m%d')

        # 初始资金
        self.capital = np.float(conf['capital'])
        print self.capital

        # 数据库的配置信息
        host = conf['host']
        port = conf['port']
        usr = conf['user']
        pwd = conf['pwd']
        db_nm = conf['db_name']

        conn = pymongo.MongoClient(host=host, port=port)
        self.db = conn[db_nm]
        self.db.authenticate(name=usr, password=pwd)

        # 将需要的数据信息存到字典self.data中，key是各个合约，value仍然是个字典，里面包含需要的字段信息
        # e.g. self.data = {'TA.CZC': {'CLOSE': [], 'date': []}}

        raw_data = conf['data']
        self.data = {}
        self.unit = conf['trade_unit']
        self.bt_mode = conf['backtest_mode']
        self.category = {}
        for d in raw_data:
            self.category[d['obj_content']] = d['commodity']
            table = self.db[d['collection']] if 'collection' in d else self.db['FuturesMD']
            query_arg = {d['obj_field']: d['obj_content'], 'date': {'$gte': self.start_dt, '$lte': self.end_dt}}
            projection_fields = ['date'] + d['fields']
            res = table.find(query_arg, projection_fields).sort('date', pymongo.ASCENDING)
            df_res = pd.DataFrame.from_records(res)
            df_res.drop(columns='_id', inplace=True)
            self.data[d['obj_content']] = df_res.to_dict(orient='list')

        # 将提取的数据按照时间的并集重新生成
        date_set = set()
        for _, v in self.data.items():
            date_set = date_set.union(v['date'])
        self.dt = np.array(list(date_set))
        self.dt.sort()

        for k, v in self.data.items():
            con = np.in1d(self.dt, v['date'])
            self.data[k].pop('date')
            for sub_k, sub_v in v.items():
                if sub_k != 'date':
                    temp = sub_v
                    self.data[k][sub_k] = np.ones(self.dt.shape) * np.nan
                    self.data[k][sub_k][con] = temp

    def strategy(self):
        raise NotImplementedError

    def getPnlDaily(self, wgtsDict):
        # 根据权重计算每日的pnl
        # wgtsDict是权重字典，key是合约名，value是该合约权重
        # 检查权重向量与时间向量长度是否一致
        for k, v in wgtsDict.items():
            if len(v) != len(self.dt):
                print u'%s的权重向量与时间向量长度不一致' % k
                raise Exception(u'权重向量与时间向量长度不一致')

        if self.bt_mode == 'OPEN':
            # 如果是开盘价进行交易，则将初始权重向后平移一位
            for k in wgtsDict:
                res = np.zeros_like(wgtsDict[k])
                res[1:] = wgtsDict[k][:-1]
                wgtsDict[k] = res

        pnl_daily = np.zeros_like(self.dt)

        holdpos = {}

        for i, v in enumerate(self.dt):
            newtradedaily = []
            mkdata = {}
            for k, val in wgtsDict.items():

                # 如果在当前日期该合约没有开始交易，则直接跳出当前循环，进入下一个合约
                if np.isnan(self.data[k]['CLOSE'][i]):
                    continue

                # 需要传入的市场数据
                mkdata[k] = {'CLOSE': self.data[k]['CLOSE'][i],
                             'OPEN': self.data[k]['OPEN'][i],
                             'multiplier': self.unit[self.category[k]]}
                # 如果不是第一天交易的话，需要前一天的收盘价
                if i != 0 and ~np.isnan(self.data[k]['CLOSE'][i-1]):
                    mkdata[k]['PRECLOSE'] = self.data[k]['CLOSE'][i-1]

                if i == 0 or np.isnan(self.data[k]['CLOSE'][i-1]):
                    if val[i] != 0:
                        newtrade = TradeRecordByTimes()
                        newtrade.setDT(v)
                        newtrade.setContract(k)
                        newtrade.setCommodity(self.category[k])
                        newtrade.setPrice(self.data[k][self.bt_mode][i])
                        newtrade.setType(1)
                        newtrade.setVolume(abs(val[i]))
                        newtrade.setMultiplier(self.unit[self.category[k]])
                        newtrade.setDirection(np.sign(val[i]))

                        newtradedaily.append(newtrade)

                else:
                    if val[i] * val[i-1] < 0:
                        newtrade1 = TradeRecordByTimes()
                        newtrade1.setDT(v)
                        newtrade1.setContract(k)
                        newtrade1.setCommodity(self.category[k])
                        newtrade1.setPrice(self.data[k][self.bt_mode][i])
                        newtrade1.setType(-1)
                        newtrade1.setVolume(abs(val[i-1]))
                        newtrade1.setMultiplier(self.unit[self.category[k]])
                        newtrade1.setDirection(np.sign(val[i]))
                        newtradedaily.append(newtrade1)

                        newtrade2 = TradeRecordByTimes()
                        newtrade2.setDT(v)
                        newtrade2.setContract(k)
                        newtrade2.setCommodity(self.category[k])
                        newtrade2.setPrice(self.data[k][self.bt_mode][i])
                        newtrade2.setType(1)
                        newtrade2.setVolume(abs(val[i]))
                        newtrade2.setMultiplier(self.unit[self.category[k]])
                        newtrade2.setDirection(np.sign(val[i]))
                        newtradedaily.append(newtrade2)

                    elif val[i] == val[i-1]:  # 没有交易
                        continue

                    else:
                        newtrade = TradeRecordByTimes()
                        newtrade.setDT(v)
                        newtrade.setContract(k)
                        newtrade.setCommodity(self.category[k])
                        newtrade.setPrice(self.data[k][self.bt_mode][i])
                        newtrade.setType(np.sign(abs(val[i]) - abs(val[i-1])))
                        newtrade.setVolume(abs(val[i] - val[i-1]))
                        newtrade.setMultiplier(self.unit[self.category[k]])
                        newtrade.setDirection(np.sign(val[i] - val[i-1]))
                        newtradedaily.append(newtrade)

            trd = TradeRecordByDay(dt=v, holdPosDict=holdpos, MkData=mkdata, newTrade=newtradedaily)
            trd.addNewPositon()
            pnl_daily[i] = trd.getFinalMK()
            holdpos = trd.getHoldPosition()

        return pnl_daily

    def getNV(self, wgtsDict):
        # 计算总的资金曲线变化情况
        return self.capital + np.cumsum(self.getPnlDaily(wgtsDict))


    def statTrade(self, wgtsDict):
        """
        对每笔交易进行统计，wgtsDict是权重字典

        """
        # 检查权重向量与时间长度是否一致
        for k, v in wgtsDict.items():
            if len(v) != len(self.dt):
                print u'%s的权重向量与时间向量长度不一致' % k
                raise Exception(u'权重向量与时间向量长度不一致')

        trade_record = {}
        for k, v in wgtsDict.items():

            trade_record[k] = []
            count = 0


            # if self.bt_mode == 'OPEN':
                # weights = np.zeros(wgts.shape)
                # weights[1:] = wgts[:-1]
                # res = self.prepareData(db=db, collection=collection, contract=contract, field=['date', 'OPEN', 'CLOSE'])
                # dt = res['date']
                # open_price = res['OPEN']
                # close_price = res['CLOSE']

                # if dt.shape != wgts.shape:
                #     raise Exception('日期向量与权重向量的长度不一致')


                # open_price = self.data[k]['OPEN']

            trade_price = self.data[k][self.bt_mode]

            unclosed = 0
            for i in range(1, len(v)):
                if np.isnan(trade_price[i]):
                    continue
                if v[i] != 0 and (i == 0 or np.isnan(trade_price[i-1])):
                    # 第一天交易就开仓
                    count += 1
                    tr_r = TradeRecordByTrade()
                    tr_r.setCounter(count)
                    tr_r.setOpen(trade_price[i])
                    tr_r.setOpenDT(self.dt[i])
                    tr_r.setVolume(abs(v[i]))
                    tr_r.setMultiplier(self.unit[self.category[k]])
                    tr_r.setContract(k)
                    tr_r.setDirection(np.sign(v[i]))
                    trade_record[k].append(tr_r)
                    unclosed += tr_r.volume
                    unclosed_dir = tr_r.direction


                elif abs(v[i]) > abs(v[i-1]) and v[i] * v[i-1] >= 0:
                    # 新开仓或加仓
                    count += 1
                    tr_r = TradeRecordByTrade()
                    tr_r.setCounter(count)
                    tr_r.setOpen(trade_price[i])
                    tr_r.setOpenDT(self.dt[i])
                    tr_r.setVolume(abs(v[i]) - abs(v[i-1]))
                    tr_r.setMultiplier(self.unit[self.category[k]])
                    tr_r.setContract(k)
                    tr_r.setDirection(np.sign(v[i]))
                    trade_record[k].append(tr_r)
                    unclosed += tr_r.volume
                    unclosed_dir = tr_r.direction

                elif abs(v[i]) < abs(v[i-1]) and v[i] * v[i-1] >= 0:

                    # 减仓或平仓（目前应该只支持一次平仓，不能计算分批减仓）
                    j = 1
                    while unclosed != 0 and - unclosed_dir == np.sign(v[i] - v[i-1]):
                        trade_record[k][-j].setClose(trade_price[i])
                        trade_record[k][-j].setCloseDT(self.dt[i])
                        trade_record[k][-j].calcHoldingPeriod()
                        trade_record[k][-j].calcPnL()
                        trade_record[k][-j].calcRtn()
                        unclosed -= trade_record[k][-j].volume
                        if unclosed == 0:
                            unclosed_dir = None
                        j += 1


                elif v[i] * v[i-1] < 0:

                    # 平仓后反方向开仓（目前只支持一次平仓）
                    j = 1
                    while unclosed != 0 and unclosed_dir == np.sign(v[i-1]):
                        trade_record[k][-j].setClose(trade_price[i])
                        trade_record[k][-j].setCloseDT(self.dt[i])
                        trade_record[k][-j].calcHoldingPeriod()
                        trade_record[k][-j].calcPnL()
                        trade_record[k][-j].calcRtn()
                        unclosed = unclosed - trade_record[k][-j].volume
                        if unclosed == 0:
                            unclosed_dir = None
                        j += 1

                    count += 1
                    tr_r = TradeRecordByTrade()
                    tr_r.setCounter(count)
                    tr_r.setOpen(trade_price[i])
                    tr_r.setOpenDT(self.dt[i])
                    tr_r.setVolume(abs(v[i]))
                    tr_r.setMultiplier(self.unit[self.category[k]])
                    tr_r.setContract(k)
                    tr_r.setDirection(np.sign(v[i]))
                    trade_record[k].append(tr_r)
                    unclosed = unclosed + tr_r.volume
                    unclosed_dir = tr_r.direction

            # elif self.bt_mode == 'CLOSE':
            #     weights = wgts.copy()
            #     res = self.prepareData(db=db, collection=collection, contract=contract, field=['date', 'CLOSE'])
            #     dt = res['date']
            #     close_price = res['CLOSE']
            #
            #     if dt.shape != wgts.shape:
            #         raise Exception('日期向量与权重向量的长度不一致')
            #
            #     trade_record = []
            #     unclosed = 0
            #
            #     for i in range(1, len(weights)):
            #
            #         if abs(weights[i]) > abs(weights[i-1]) and weights[i] * weights[i-1] >= 0:
            #
            #             # 新开仓
            #             count += 1
            #             tr_r = TradeRecord()
            #             tr_r.setCounter(count)
            #             tr_r.setOpen(close_price[i])
            #             tr_r.setOpenDT(dt[i])
            #             tr_r.setVolume(abs(weights[i]) - abs(weights[i-1]))
            #             tr_r.setMultiplier(multiplier)
            #             tr_r.setContract(contract)
            #             tr_r.setDirection(np.sign(weights[i]))
            #             trade_record.append(tr_r)
            #             unclosed += tr_r.volume
            #             unclosed_dir = tr_r.direction
            #
            #         elif abs(weights[i]) < abs(weights[i-1]) and weights[i] * weights[i-1] >= 0:
            #
            #             # 减仓或平仓
            #             j = 1
            #             while unclosed != 0 and - unclosed_dir == np.sign(weights[i] - weights[i-1]):
            #                 trade_record[-j].setClose(close_price[i])
            #                 trade_record[-j].setCloseDT(dt[i])
            #                 trade_record[-j].calcPnL()
            #                 unclosed -= trade_record[-j].volume
            #                 if unclosed == 0:
            #                     unclosed_dir = None
            #                 j += 1
            #
            #         elif weights[i] * weights[i-1] < 0:
            #
            #             # 平仓后反方向开仓
            #             j = 1
            #             while unclosed != 0 and unclosed_dir == np.sign(weights[i-1]):
            #                 trade_record[-j].setClose(close_price[i])
            #                 trade_record[-j].setCloseDT(dt[i])
            #                 trade_record[-j].calcPnL()
            #                 unclosed -= trade_record[-j].volume
            #                 if unclosed == 0:
            #                     unclosed_dir = None
            #                 j += 1
            #
            #             count += 1
            #             tr_r = TradeRecord()
            #             tr_r.setCounter(count)
            #             tr_r.setOpen(close_price[i])
            #             tr_r.setOpenDT(dt[i])
            #             tr_r.setVolume(abs(weights[i]))
            #             tr_r.setMultiplier(multiplier)
            #             tr_r.setContract(contract)
            #             tr_r.setDirection(np.sign(weights[i]))
            #             trade_record.append(tr_r)
            #             unclosed += tr_r.volume
            #             unclosed_dir = tr_r.direction

            trade_times = len(trade_record[k])
            buy_times = len([t for t in trade_record[k] if t.direction == 1])
            sell_times = len([t for t in trade_record[k] if t.direction == -1])
            profit_times = len([t for t in trade_record[k] if t.pnl > 0])
            loss_times = len([t for t in trade_record[k] if t.pnl < 0])
            buy_pnl = [t.pnl for t in trade_record[k] if t.direction == 1]
            sell_pnl = [t.pnl for t in trade_record[k] if t.direction == -1]

            trade_rtn = [t.rtn for t in trade_record[k]]
            trade_holding_period = [t.holding_period for t in trade_record[k]]

            if buy_times == 0:
                buy_avg_pnl = np.nan
            else:
                buy_avg_pnl = sum(buy_pnl) / buy_times
            if sell_times == 0:
                sell_avg_pnl = np.nan
            else:
                sell_avg_pnl = sum(sell_pnl) / sell_times

            print '+++++++++++++++%s合约交易统计++++++++++++++++++++' % k
            print '交易次数: %d' % trade_times
            print '做多次数: %d' % buy_times
            print '做空次数: %d' % sell_times
            print '盈利次数: %d' % profit_times
            print '亏损次数: %d' % loss_times
            print '做多平均盈亏: %f' % buy_avg_pnl
            print '做空平均盈亏: %f' % sell_avg_pnl
            print '平均每笔交易收益率(不考虑杠杆): %f' % np.nanmean(trade_rtn)
            print '平均年化收益率(不考虑杠杆): %f' % (np.nansum(trade_rtn) * 250. / np.nansum(trade_holding_period))

        return trade_record


    # def calc_pnl_old(self, db, collection, contract, multiplier, wgts, tcost_type, tcost):
    #     """
    #     根据生成的权重计算盈亏情况，需要的数据，db是数据库，collection是数据表，contract是合约，multiplier是合约乘数。
    #     然后根据wgts权重向量计算。需要注意的问题是权重向量与时间向量的长度要相同。
    #     tcost是交易成本，期货交易成本有两种，一种是根据成交金额的百分比，另一种是每手交易收取固定费用。tcost_type=1时，为不固定费
    #     用，tcost是百分比。tcost_type=2时，为固定费用，tcost是每笔收取的费用。 tcost_type=0时，不计手续费
    #     """
    #
    #     if self.bt_mode == 'NextOpen':
    #         weights = np.zeros(wgts.shape)
    #         weights[1:] = wgts[:-1]
    #         res = self.prepareData(db=db, collection=collection, contract=contract, field=['date', 'OPEN', 'CLOSE'])
    #         dt = res['date']
    #         open_price = res['OPEN']
    #         close_price = res['CLOSE']
    #
    #         if dt.shape != wgts.shape:
    #             raise Exception('日期向量与权重向量的长度不一致')
    #
    #         pnl = np.zeros_like(wgts)
    #
    #         for i in range(1, len(weights)):
    #
    #             # 当天增减仓
    #             if weights[i] * weights[i-1] >= 0:
    #                 delta_wgt = weights[i] - weights[i-1]
    #
    #                 if tcost_type == 1:
    #                     trade_cost = open_price[i] * abs(delta_wgt) * multiplier * tcost
    #                 elif tcost_type == 2:
    #                     trade_cost = abs(delta_wgt) * tcost
    #                 elif tcost_type == 0:
    #                     trade_cost = 0
    #
    #
    #                 pnl[i] = pnl[i-1] + (close_price[i] - open_price[i]) * delta_wgt * multiplier + \
    #                          (close_price[i] - close_price[i-1]) * weights[i-1] * multiplier - trade_cost
    #             # 当天改变持仓方向
    #             elif weights[i] * weights[i-1] < 0:
    #
    #                 if tcost_type == 1:
    #                     trade_cost = open_price[i] * (abs(weights[i]) + abs(weights[i-1])) * multiplier * tcost
    #                 elif tcost_type == 2:
    #                     trade_cost = (abs(weights[i]) + abs(weights[i-1])) * tcost
    #                 elif tcost_type == 0:
    #                     trade_cost = 0
    #
    #                 pnl[i] = pnl[i-1] + (close_price[i] - open_price[i]) * weights[i] * multiplier + \
    #                          (open_price[i] - close_price[i-1]) * weights[i-1] * multiplier - trade_cost
    #
    #     elif self.bt_mode == 'CLOSE':
    #         weights = wgts.copy()
    #         res = self.prepareData(db=db, collection=collection, contract=contract, field=['date', 'CLOSE'])
    #         dt = res['date']
    #         close_price = res['CLOSE']
    #
    #         if dt.shape != wgts.shape:
    #             raise Exception('日期向量与权重向量的长度不一致')
    #
    #         pnl = np.zeros_like(wgts)
    #
    #         for i in range(1, len(weights)):
    #
    #             delta_wgt = weights[i] - weights[i-1]
    #
    #             if tcost_type == 1:
    #                 trade_cost = close_price[i] * abs(delta_wgt) * multiplier * tcost
    #             elif tcost_type == 2:
    #                 trade_cost = abs(delta_wgt) * tcost
    #             elif tcost_type == 0:
    #                 trade_cost = 0
    #
    #             pnl[i] = pnl[i-1] + (close_price[i] - close_price[i-1]) * weights[i-1] * multiplier - trade_cost
    #
    #     pnl_df = pd.DataFrame({'pnl': pnl}, index=dt)
    #     return pnl_df
    #
    #
    # def order_combination(self, trade_list):
    #     """进行订单合并，根据交易的合约和时间进行，暂时先不需要"""
    #     trade_dict = {}
    #     for t in trade_list:
    #         if t.contract not in trade_dict:
    #             trade_dict[t.contract] = {}
    #         if t.open_dt not in trade_dict[t.contract]:
    #             trade_dict[t.contract][t.open_dt] = t.volume * t.direction
    #         else:
    #             trade_dict[t.contract][t.open_dt] = trade_dict[t.contract][t.open_dt] + t.volume * t.direction
    #
    #         if t.close_dt not in trade_dict[t.contract]:
    #             trade_dict[t.contract][t.close_dt] = t.volume * t.direction * (-1)
    #         else:
    #             trade_dict[t.contract][t.close_dt] = trade_dict[t.contract][t.close_dt] - t.volume * t.direction
    #
    #     trade_comb = sorted(trade_dict.items(), key=lambda x: x[0][0], reverse=False)
    #
    # def prepareData(self, db, collection, contract, field=['date', 'CLOSE']):
    #
    #     col = self.useDBCollections(db=db, collection=collection)
    #
    #     if not self.start_date:
    #         self.start_date = datetime.strptime('20000101', '%Y%m%d')
    #     if not self.end_date:
    #         self.end_date = datetime.today()
    #     # if not self.lookback:
    #     #     self.lookback = 0
    #
    #     if isinstance(self.start_date, str):
    #         self.start_date = datetime.strptime(self.start_date, '%Y%m%d')
    #     if isinstance(self.end_date, str):
    #         self.end_date = datetime.strptime(self.end_date, '%Y%m%d')
    #
    #     # queryArgs = {'wind_code': contract}
    #     # projectionField = ['date']
    #     #
    #     # res = col.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING).limit(1)
    #     #
    #     # contract_init = list(res)[0]['date']
    #
    #     # if self.start_date < contract_init:
    #     #     self.init_date = contract_init
    #     # elif self.lookback == 0:
    #     #     self.init_date = self.start_date
    #     # else:
    #     #     queryArgs = {'wind_code': contract,
    #     #                  'date': {'$lt': self.start_date}}
    #     #     projectionField = ['date']
    #     #     res = col.find(queryArgs, projectionField).sort('date', pymongo.DESCENDING).limit(self.lookback)
    #     #     self.init_date = list(res)[-1]['date']
    #
    #     queryArgs = {'wind_code': contract,
    #                  'date': {'$gte': self.start_date, '$lte': self.end_date}}
    #     projectionField = field
    #     res = col.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
    #     preData = dict()
    #
    #     for f in projectionField:
    #         preData[f] = []
    #
    #     for r in res:
    #         for f in projectionField:
    #             preData[f].append(r[f])
    #
    #     for f in projectionField:
    #         preData[f] = np.array(preData[f])
    #     return preData

    def showBTResult(self, net_value):
        rtn, vol, sharpe, dd = self.calcIndicator(net_value)
        print '年化收益率：', rtn
        print '年化波动率：', vol
        print '夏普比率：', sharpe
        print '最大回撤：', dd

    # def calcRtn(self, net_value):
    #     rtn_daily = np.ones(len(net_value)) * np.nan
    #     rtn_daily[1:] = net_value[1:] / net_value[:-1] - 1.
    #     return rtn_daily

    def calcIndicator(self, net_value):
        rtn_daily = np.ones(len(net_value)) * np.nan
        rtn_daily[1:] = net_value[1:] / net_value[:-1] - 1.
        annual_rtn = np.nanmean(rtn_daily) * 250
        annual_std = np.nanstd(rtn_daily) * np.sqrt(250)
        sharpe = annual_rtn / annual_std

        index_end = np.argmax(np.maximum.accumulate(net_value) - net_value)
        index_start = np.argmax(net_value[:index_end])
        max_drawdown = net_value[index_end] - net_value[index_start]

        return annual_rtn, annual_std, sharpe, max_drawdown

    # def calcAnnualSTD(self):
    #     return np.nanstd(self.rtn_daily) * np.sqrt(250)
    #
    # def calcSharpe(self):
    #     return self.calcAnnualRTN() / self.calcAnnualSTD()

    # def calcMaxDrawdown(self, net_value):
    #     index_end = np.argmax(np.maximum.accumulate(net_value) - net_value)
    #     index_start = np.argmax(net_value[:index_end])
    #     max_drawdown = net_value[index_end] - net_value[index_start]
    #     return max_drawdown




if __name__ == '__main__':

    ttest1 = TradeRecordByTimes()
    ttest1.setDT('20181203')
    ttest1.setContract('TA1901.CZC')
    ttest1.setPrice(6000)
    ttest1.setVolume(10)
    ttest1.setDirection(1)
    ttest1.setType(1)
    ttest1.setMultiplier(5)
    ttest1.setMarginRatio(0.1)
    ttest1.calMarginOccupation()

    # tdtest = TradeRecordByDay(ttest1)

    print ttest1

    a1 = {'TA1901.CZC': {}}
    a2 = dict()
    a = TradeRecordByDay(dt='20181203', holdPosDict=a2, MkData=a2, newTrade=[])
    a.addNewPositon()
    print a.getFinalMK()
    print a.getHoldPosition()
    # a = BacktestSys()
    # print a.net_value
    # print a.calcSharpe()
    # print a.calcMaxDrawdown()
    # a.generateNV()
    # print a.calcBTResult()
    # plt.plot(a.net_value)
    # plt.show()
    # a.start_date = '20180101'
    # a.prepareData(db='FuturesDailyWind', collection='TA.CZC_Daily', contract='TA.CZC')
