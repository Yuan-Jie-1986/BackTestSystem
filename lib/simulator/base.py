# coding=utf-8

import sys
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
        self.trade_commodity_value = None  # 合约价值，一定是正值
        self.trade_cost_mode = None  # 手续费收取方式：percentage还是fixed
        self.trade_cost_unit = None  # 手续费
        self.trade_cost = 0.

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

    def setCost(self, mode, value):
        self.trade_cost_mode = mode
        self.trade_cost_unit = value

    def calMarginOccupation(self):
        self.trade_margin_occupation = self.trade_price * self.trade_multiplier * self.trade_margin_ratio * \
                                       self.trade_volume

    def calValue(self):
        self.trade_commodity_value = self.trade_price * self.trade_multiplier * abs(self.trade_volume)

    def calCost(self):
        if self.trade_cost_mode == 'percentage':
            self.trade_cost = self.trade_price * self.trade_volume * self.trade_multiplier * self.trade_cost_unit
        elif self.trade_cost_mode == 'fixed':
            self.trade_cost = self.trade_volume * self.trade_cost_unit


# 逐笔的交易记录
class TradeRecordByTrade(object):

    def __init__(self):
        self.open = np.nan
        self.open_dt = None
        self.close = np.nan
        self.close_dt = None
        self.volume = np.nan
        self.direction = np.nan
        self.contract = ''
        self.commodity = ''
        self.multiplier = np.nan
        self.count = np.nan
        self.pnl = np.nan
        self.rtn = np.nan
        self.holding_period = np.nan  # 该交易的交易周期
        self.tcost_mode = None
        self.tcost_unit = np.nan
        self.tcost = 0

    def calcPnL(self):
        self.pnl = (self.close - self.open) * self.volume * self.multiplier * self.direction - self.tcost

    def calcRtn(self):
        self.calcPnL()
        self.rtn = self.pnl / (self.open * self.volume * self.multiplier)
        # self.rtn = self.direction * ((self.close / self.open) - 1.)

    def calcPnlDeductedCost(self):
        self.calcTcost()
        self.calcPnL()
        self.pnl_deducted_cost = self.pnl - self.tcost

    def calcRtnDeductedCost(self):
        self.calcPnlDeductedCost()
        self.rtn_deducted_cost = self.pnl_deducted_cost / (self.open * self.volume * self.multiplier)


    def calcHoldingPeriod(self):
        self.holding_period = (self.close_dt - self.open_dt + timedelta(1)).days

    def calcTcost(self):
        if self.tcost_mode == 'percentage':
            self.tcost = (self.open + self.close) * self.volume * self.multiplier * self.tcost_unit
        elif self.tcost_mode == 'fixed':
            self.tcost = self.volume * 2. * self.tcost_unit


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

    def setTcost(self, mode, value):
        self.tcost_mode = mode
        self.tcost_unit = value


# 逐日的交易记录
class TradeRecordByDay(object):

    def __init__(self, dt, holdPosDict, MkData, newTrade):
        self.dt = dt  # 日期
        self.newTrade = newTrade  # 当天进行的交易
        self.mkdata = MkData  # 合约市场数据
        self.holdPosition = holdPosDict  # 之前已有的持仓, 字典中的volume的值是有正有负，正值代表持多仓，负值为空仓
        self.daily_pnl = 0  # 每日的pnl
        self.daily_margin_occ = 0  # 每日的保证金占用情况
        self.daily_commodity_value = 0  # 每日的合约价值

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
                    print self.dt, self.mkdata[k]
                    raise Exception(u'请检查传入的市场数据是否正确')

                new_pnl = v['volume'] * (self.mkdata[k]['CLOSE'] - self.mkdata[k]['PRECLOSE']) * \
                          self.mkdata[k]['multiplier']

                # 如果某些品种当天没有成交量，那么算出来的结果可能为nan
                if np.isnan(new_pnl):
                    print self.dt
                    raise Exception(u'请检查当天的量价数据是否有问题')

                self.daily_pnl += new_pnl

            else:
                self.holdPosition[k]['volume'] = 0

            if 'newTrade' in v:

                for nt in v['newTrade']:

                    new_pnl = nt.trade_volume * nt.trade_direction * nt.trade_multiplier * \
                              (self.mkdata[k]['CLOSE'] - nt.trade_price)

                    # 如果某些品种当天没有成交量，那么算出来的结果可能为nan
                    if np.isnan(new_pnl):
                        print self.dt, nt.__dict__
                        raise Exception(u'请检查当天的量价数据是否有问题')
                    if np.isnan(nt.trade_cost):
                        print self.dt, nt.__dict__
                        raise Exception(u'交易费用为nan，请检查当天的量价数据是否有问题')

                    self.daily_pnl = self.daily_pnl + new_pnl - nt.trade_cost
                    self.holdPosition[k]['volume'] = self.holdPosition[k]['volume'] + nt.trade_volume * \
                                                     nt.trade_direction

                del self.holdPosition[k]['newTrade']

            if self.holdPosition[k]['volume'] == 0:
                del self.holdPosition[k]

            else:
                new_margin_occ = abs(self.holdPosition[k]['volume']) * self.mkdata[k]['CLOSE'] * \
                                 self.mkdata[k]['multiplier'] * self.mkdata[k]['margin_ratio']
                new_commodity_value = abs(self.holdPosition[k]['volume']) * self.mkdata[k]['CLOSE'] * \
                                      self.mkdata[k]['multiplier']

                self.daily_margin_occ += new_margin_occ
                self.daily_commodity_value += new_commodity_value

        return self.daily_pnl, self.daily_margin_occ, self.daily_commodity_value

    def getHoldPosition(self):
        return self.holdPosition


class BacktestSys(object):

    def __init__(self):
        self.current_file = sys.argv[0]
        self.prepare()

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
        self.margin_ratio = conf['margin_ratio']
        self.category = {}
        self.tcost = conf['tcost']

        if self.tcost:
            self.tcost_list = conf['tcost_list']

        for d in raw_data:
            self.category[d['obj_content']] = d['commodity']
            table = self.db[d['collection']] if 'collection' in d else self.db['FuturesMD']
            query_arg = {d['obj_field']: d['obj_content'], 'date': {'$gte': self.start_dt, '$lte': self.end_dt}}
            projection_fields = ['date'] + d['fields']
            res = table.find(query_arg, projection_fields).sort('date', pymongo.ASCENDING)
            df_res = pd.DataFrame.from_records(res)
            df_res.drop(columns='_id', inplace=True)
            self.data[d['obj_content']] = df_res.to_dict(orient='list')

        # 将提取的数据按照交易时间的并集重新生成
        date_set = set()
        for _, v in self.data.items():
            date_set = date_set.union(v['date'])
        self.dt = np.array(list(date_set))
        self.dt.sort()

        # 如果定义了date_type，则去调取交易日期序列
        if 'date_type' in conf:
            self.date_type = conf['date_type']
            table = self.db['DateDB']
            query_arg = {'exchange': self.date_type, 'date': {'$gte': self.start_dt, '$lte': self.end_dt}}
            projection_fields = ['date']
            res = table.find(query_arg, projection_fields)
            df_res = pd.DataFrame.from_records(res)
            trading_dt = df_res['date'].values
            trading_dt = np.array([pd.Timestamp(dt) for dt in trading_dt])
            dt_con = np.in1d(self.dt, trading_dt)
            self.dt = self.dt[dt_con]

        # 根据交易日期序列重新整理数据
        for k, v in self.data.items():
            con_1 = np.in1d(self.dt, v['date'])
            con_2 = np.in1d(v['date'], self.dt)
            self.data[k].pop('date')
            for sub_k, sub_v in v.items():
                if sub_k != 'date':
                    self.data[k][sub_k] = np.ones(self.dt.shape) * np.nan
                    self.data[k][sub_k][con_1] = np.array(sub_v)[con_2]

        # 对当天没有交易的品种的OPEN进行修正处理
        for k in self.data:
            try:
                self.data[k]['OPEN'][np.isnan(self.data[k]['OPEN'])] = self.data[k]['CLOSE'][np.isnan(self.data[k]['OPEN'])]
            except KeyError:
                continue

        #
        # # 对某些数据进行修正
        # # 沥青数据
        # dt_error = [datetime(2014, 12, 2), datetime(2014, 12, 15), datetime(2014, 12, 19), datetime(2014, 12, 22),
        #             datetime(2014, 12, 23)]
        # for d in dt_error:
        #     try:
        #         self.data['BU.SHF']['OPEN'][self.dt == d] = self.data['BU.SHF']['CLOSE'][self.dt == d]
        #     except KeyError, e:
        #         pass


    def strategy(self):
        raise NotImplementedError

    def wgtsProcess(self, wgtsDict):
        """对生成的权重进行处理，需要注意的是这个函数要最后再用"""
        if self.bt_mode == 'OPEN':
            # 如果是开盘价进行交易，则将初始权重向后平移一位
            for k in wgtsDict:
                res = np.zeros(len(wgtsDict[k]) + 1)
                res[1:] = wgtsDict[k]
                wgtsDict[k] = res
        return wgtsDict

    def wgtsStandardization(self, wgtsDict):
        """根据给定的权重重新生成标准化的权重：将所有初始资金全部使用，而不加杠杆。各合约之间的比例根据合约价值的倒数的比例来确定"""

        # for i in np.arange(len(self.dt)):
        #     for k in wgtsDict:
        #
        #
        #         print self.dt[i], k, self.data[k]['CLOSE'][i]

        # 权重的dataframe
        wgts_df = pd.DataFrame.from_dict(wgtsDict)
        wgts_df.index = self.dt
        cls = dict()
        for k in wgtsDict:
            cls[k] = self.data[k]['CLOSE']
        cls_df = pd.DataFrame.from_dict(cls)
        cls_df.index = self.dt
        for c in cls_df:
            cls_df[c] *= self.unit[self.category[c]]
        cls_df = cls_df * np.sign(abs(wgts_df))  # 合约价值与是否持仓相乘
        cls_df[cls_df == 0] = np.nan
        value_df = 1e6 / cls_df  # 合约价值的倒数
        value_min = value_df.min(axis=1, skipna=True)
        ratio_df = pd.DataFrame()
        for c in value_df:
            ratio_df[c] = value_df[c] / value_min

        value_df = cls_df * ratio_df
        base_series = self.capital / value_df.sum(axis=1, skipna=True)
        base_series[np.isinf(base_series)] = 0.
        for c in ratio_df:
            ratio_df[c] *= base_series
        ratio_df.fillna(0, inplace=True)
        ratio_df = ratio_df * np.sign(wgts_df)
        ratio_df = ratio_df.astype('float')
        ratio_df = ratio_df.round(decimals=0)
        wgtsDict = ratio_df.to_dict(orient='list')
        return wgtsDict

    def getPnlDaily(self, wgtsDict):
        # 根据权重计算每日的pnl，每日的保证金占用，每日的合约价值
        # wgtsDict是权重字典，key是合约名，value是该合约权重
        # 检查权重向量与时间向量长度是否一致
        # 需要注意的问题是传入的参数如果是字典，那么可能会改变该字典
        for k, v in wgtsDict.items():
            if len(v) != len(self.dt):
                print u'%s的权重向量与时间向量长度不一致，%s的权重向量长度为%d，时间向量的长度为%d' % (k, k, len(v), len(self.dt))
                raise Exception(u'权重向量与时间向量长度不一致')
        pnl_daily = np.zeros_like(self.dt).astype('float')
        margin_occ_daily = np.zeros_like(self.dt).astype('float')
        value_daily = np.zeros_like(self.dt).astype('float')

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
                             'multiplier': self.unit[self.category[k]],
                             'margin_ratio': self.margin_ratio[k]}

                # 如果不是第一天交易的话，需要前一天的收盘价
                if i != 0 and ~np.isnan(self.data[k]['CLOSE'][i-1]):
                    mkdata[k]['PRECLOSE'] = self.data[k]['CLOSE'][i-1]

                # 合约首日交易便有持仓时
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
                        if self.tcost:
                            newtrade.setCost(**self.tcost_list[k])
                        newtrade.calCost()
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
                        if self.tcost:
                            newtrade1.setCost(**self.tcost_list[k])
                        newtrade1.calCost()
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
                        if self.tcost:
                            newtrade2.setCost(**self.tcost_list[k])
                        newtrade2.calCost()
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
                        if self.tcost:
                            newtrade.setCost(**self.tcost_list[k])
                        newtrade.calCost()
                        newtradedaily.append(newtrade)

            trd = TradeRecordByDay(dt=v, holdPosDict=holdpos, MkData=mkdata, newTrade=newtradedaily)
            trd.addNewPositon()
            pnl_daily[i], margin_occ_daily[i], value_daily[i] = trd.getFinalMK()
            holdpos = trd.getHoldPosition()

        return pnl_daily, margin_occ_daily, value_daily

    def getNV(self, wgtsDict):
        # 计算总的资金曲线变化情况
        return self.capital + np.cumsum(self.getPnlDaily(wgtsDict)[0])

    def statTrade(self, wgtsDict):
        """
        对每笔交易进行统计，wgtsDict是权重字典

        """
        # 检查权重向量与时间长度是否一致
        for k, v in wgtsDict.items():
            if len(v) != len(self.dt):
                print u'%s的权重向量与时间向量长度不一致' % k
                raise Exception(u'权重向量与时间向量长度不一致')
        # total_pnl = 0.
        trade_record = {}
        uncovered_record = {}
        for k, v in wgtsDict.items():

            trade_record[k] = []
            uncovered_record[k] = []
            count = 0
            trade_price = self.data[k][self.bt_mode]

            for i in range(len(v)):

                if np.isnan(trade_price[i]) or (i == 0 and v[i] == 0):
                    # 需要注意的是如果当天没有成交量，可能一些价格会是nan值，会导致回测计算结果不准确
                    continue

                elif (v[i] != 0 and i == 0) or (v[i] != 0 and np.isnan(trade_price[i-1]) and i != 0 and v[i-1] == 0):
                    # 第一天交易就开仓
                    # 第二种情况是为了排除该品种当天没有交易，价格为nan的这种情况，e.g. 20141202 BU.SHF
                    count += 1
                    tr_r = TradeRecordByTrade()
                    tr_r.setCounter(count)
                    tr_r.setOpen(trade_price[i])
                    tr_r.setOpenDT(self.dt[i])
                    tr_r.setCommodity(self.category[k])
                    tr_r.setVolume(abs(v[i]))
                    tr_r.setMultiplier(self.unit[self.category[k]])
                    tr_r.setContract(k)
                    tr_r.setDirection(np.sign(v[i]))
                    if self.tcost:
                        tr_r.setTcost(**self.tcost_list[k])
                    trade_record[k].append(tr_r)
                    uncovered_record[k].append(tr_r.count)

                elif abs(v[i]) > abs(v[i-1]) and v[i] * v[i-1] >= 0:
                    # 新开仓或加仓
                    count += 1
                    tr_r = TradeRecordByTrade()
                    tr_r.setCounter(count)
                    tr_r.setOpen(trade_price[i])
                    tr_r.setOpenDT(self.dt[i])
                    tr_r.setCommodity(self.category[k])
                    tr_r.setVolume(abs(v[i]) - abs(v[i-1]))
                    tr_r.setMultiplier(self.unit[self.category[k]])
                    tr_r.setContract(k)
                    tr_r.setDirection(np.sign(v[i]))
                    if self.tcost:
                        tr_r.setTcost(**self.tcost_list[k])
                    trade_record[k].append(tr_r)
                    uncovered_record[k].append(tr_r.count)

                elif abs(v[i]) < abs(v[i-1]) and v[i] * v[i-1] >= 0:

                    # 减仓或平仓
                    needed_covered = abs(v[i - 1]) - abs(v[i])  # 需要减仓的数量
                    uncovered_record_sub = []
                    uncovered_record_add = []
                    for j in np.arange(1, len(uncovered_record[k]) + 1):
                        for m in np.arange(1, len(trade_record[k]) + 1):
                            if uncovered_record[k][-j] == trade_record[k][-m].count:
                                # 如果需要减仓的数量小于最近的开仓的数量
                                if needed_covered < trade_record[k][-m].volume:
                                    uncovered_vol = trade_record[k][-m].volume - needed_covered
                                    trade_record[k][-m].setVolume(needed_covered)
                                    trade_record[k][-m].setClose(trade_price[i])
                                    trade_record[k][-m].setCloseDT(self.dt[i])
                                    trade_record[k][-m].calcHoldingPeriod()
                                    trade_record[k][-m].calcTcost()
                                    trade_record[k][-m].calcPnL()
                                    trade_record[k][-m].calcRtn()
                                    uncovered_record_sub.append(uncovered_record[k][-j])

                                    needed_covered = 0.

                                    # 对于没有平仓的部分新建交易记录
                                    count += 1
                                    tr_r = TradeRecordByTrade()
                                    tr_r.setCounter(count)
                                    tr_r.setOpen(trade_record[k][-m].open)
                                    tr_r.setOpenDT(trade_record[k][-m].open_dt)
                                    tr_r.setCommodity(self.category[k])
                                    tr_r.setVolume(uncovered_vol)
                                    tr_r.setMultiplier(self.unit[self.category[k]])
                                    tr_r.setContract(k)
                                    tr_r.setDirection(trade_record[k][-m].direction)
                                    if self.tcost:
                                        tr_r.setTcost(**self.tcost_list[k])
                                    trade_record[k].append(tr_r)
                                    uncovered_record_add.append(tr_r.count)

                                    break


                                # 如果需要减仓的数量等于最近的开仓数量
                                elif needed_covered == trade_record[k][-m].volume:
                                    trade_record[k][-m].setClose(trade_price[i])
                                    trade_record[k][-m].setCloseDT(self.dt[i])
                                    trade_record[k][-m].calcHoldingPeriod()
                                    trade_record[k][-m].calcTcost()
                                    trade_record[k][-m].calcPnL()
                                    trade_record[k][-m].calcRtn()
                                    uncovered_record_sub.append(uncovered_record[k][-j])
                                    needed_covered = 0.
                                    break

                                # 如果需要减仓的数量大于最近的开仓数量
                                elif needed_covered > trade_record[k][-m].volume:
                                    trade_record[k][-m].setClose(trade_price[i])
                                    trade_record[k][-m].setCloseDT(self.dt[i])
                                    trade_record[k][-m].calcHoldingPeriod()
                                    trade_record[k][-m].calcTcost()
                                    trade_record[k][-m].calcPnL()
                                    trade_record[k][-m].calcRtn()
                                    uncovered_record_sub.append(uncovered_record[k][-j])
                                    needed_covered -= trade_record[k][-m].volume
                                    break

                        if needed_covered == 0.:
                            for tr in uncovered_record_sub:
                                uncovered_record[k].remove(tr)
                            for tr in uncovered_record_add:
                                uncovered_record[k].append(tr)
                            break

                elif v[i] * v[i-1] < 0:

                    # 先平仓后开仓
                    needed_covered = abs(v[i - 1])  # 需要减仓的数量
                    uncovered_record_sub = []
                    for j in np.arange(1, len(uncovered_record[k]) + 1):
                        for m in np.arange(1, len(trade_record[k]) + 1):
                            if uncovered_record[k][-j] == trade_record[k][-m].count:
                                # 如果需要减仓的数量小于最近的开仓的数量，会报错
                                if needed_covered < trade_record[k][-m].volume:
                                    raise Exception(u'请检查，待减仓的数量为什么会小于已经开仓的数量')

                                # 如果需要减仓的数量等于最近的开仓数量
                                elif needed_covered == trade_record[k][-m].volume:
                                    trade_record[k][-m].setClose(trade_price[i])
                                    trade_record[k][-m].setCloseDT(self.dt[i])
                                    trade_record[k][-m].calcHoldingPeriod()
                                    trade_record[k][-m].calcTcost()
                                    trade_record[k][-m].calcPnL()
                                    trade_record[k][-m].calcRtn()
                                    uncovered_record_sub.append(uncovered_record[k][-j])
                                    needed_covered = 0.

                                    break

                                # 如果需要减仓的数量大于最近的开仓数量
                                elif needed_covered > trade_record[k][-m].volume:
                                    trade_record[k][-m].setClose(trade_price[i])
                                    trade_record[k][-m].setCloseDT(self.dt[i])
                                    trade_record[k][-m].calcHoldingPeriod()
                                    trade_record[k][-m].calcTcost()
                                    trade_record[k][-m].calcPnL()
                                    trade_record[k][-m].calcRtn()
                                    uncovered_record_sub.append(uncovered_record[k][-j])
                                    needed_covered -= trade_record[k][-m].volume

                                    break

                        if needed_covered == 0.:
                            for tr in uncovered_record_sub:
                                uncovered_record[k].remove(tr)
                            break
                    if uncovered_record[k]:
                        print self.dt[i], k, uncovered_record[k][0]
                        raise Exception(u'请检查，依然有未平仓的交易，无法新开反向仓')

                    count += 1
                    tr_r = TradeRecordByTrade()
                    tr_r.setCounter(count)
                    tr_r.setOpen(trade_price[i])
                    tr_r.setOpenDT(self.dt[i])
                    tr_r.setCommodity(self.category[k])
                    tr_r.setVolume(abs(v[i]))
                    tr_r.setMultiplier(self.unit[self.category[k]])
                    tr_r.setContract(k)
                    tr_r.setDirection(np.sign(v[i]))
                    if self.tcost:
                        tr_r.setTcost(**self.tcost_list[k])
                    trade_record[k].append(tr_r)
                    uncovered_record[k].append(tr_r.count)

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
                buy_avg_pnl = np.nansum(buy_pnl) / buy_times
            if sell_times == 0:
                sell_avg_pnl = np.nan
            else:
                sell_avg_pnl = np.nansum(sell_pnl) / sell_times

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

            # total_pnl_k = np.nansum([tr.pnl for tr in trade_record[k]])
            # total_pnl += total_pnl_k
            # print total_pnl

        return trade_record

    def displayResult(self, wgtsDict, saveLocal=True):
        # 需要注意的一个问题是，如果在getPnlDaily函数中改变了wgtsDict，那么之后所用的wgtsDict就都改变了
        # saveLocal是逻辑变量，是否将结果存在本地
        if self.bt_mode == 'OPEN':
            new_WgtsDict = {}
            for k in wgtsDict:
                new_WgtsDict[k] = wgtsDict[k][-1]
                wgtsDict[k] = wgtsDict[k][:-1]
        pnl, margin_occ, value = self.getPnlDaily(wgtsDict)
        nv = 1. + np.cumsum(pnl) / self.capital  # 转换成初始净值为1
        margin_occ_ratio = margin_occ / (self.capital + np.cumsum(pnl))
        # leverage = value / (self.capital + np.cumsum(pnl))
        leverage = value / self.capital
        trade_record = self.statTrade(wgtsDict)
        self.showBTResult(nv)

        trade_pnl = []
        for tr in trade_record:
            trade_pnl.extend([t.pnl for t in trade_record[tr]])

        if saveLocal:
            current_file = sys.argv[0]
            save_path = os.path.splitext(current_file)[0]
            if not os.path.exists(save_path):
                os.makedirs(save_path)
            # 保存权重为wgts.csv
            wgts_df = pd.DataFrame.from_dict(wgtsDict)
            wgts_df.index = self.dt
            wgts_df.to_csv(os.path.join(save_path, 'wgts.csv'))

            # 保存总的回测结果为result.csv
            total_df = pd.DataFrame({'每日PnL': pnl, '净值': nv, '资金占用比例': margin_occ_ratio, '杠杆倍数': leverage},
                                    index=self.dt)
            total_df.to_csv(os.path.join(save_path, 'result.csv'))


            # 保存交易记录为trade_detail.csv
            detail_df = pd.DataFrame()
            for k in trade_record:
                detail_df = pd.concat([detail_df, pd.DataFrame.from_records([tr.__dict__ for tr in trade_record[k]])],
                                      ignore_index=True)
            detail_df.to_csv(os.path.join(save_path, 'details.csv'))

            # 保存最新的权重
            if self.bt_mode == 'OPEN':
                new_wgt = pd.DataFrame.from_dict(new_WgtsDict, orient='index', columns=['WGTS'])
                new_wgt.sort_values(by='WGTS', ascending=False, inplace=True)
                new_wgt.to_csv(os.path.join(save_path, 'new_wgts.csv'))


        trade_pnl = np.array(trade_pnl)
        plt.subplot(411)
        plt.plot_date(self.dt, nv, fmt='-r', label='PnL')
        plt.grid()
        plt.legend()

        plt.subplot(412)
        plt.hist(trade_pnl[~np.isnan(trade_pnl)], bins=50, label='DistOfPnL', color='r')
        plt.legend()
        plt.grid()

        plt.subplot(413)
        plt.plot_date(self.dt, margin_occ_ratio, fmt='-r', label='margin_occupation_ratio')
        plt.grid()
        plt.legend()


        plt.subplot(414)
        plt.plot_date(self.dt, leverage, fmt='-r', label='leverage')
        plt.grid()
        plt.legend()

        plt.show()

    def showBTResult(self, net_value):
        rtn, vol, sharpe, dd, dds, dde = self.calcIndicator(net_value)
        print '==============回测结果==============='
        print '年化收益率：', rtn
        print '年化波动率：', vol
        print '夏普比率：', sharpe
        print '最大回撤：', dd
        print '最大回撤起始时间：', dds
        print '最大回撤结束时间：', dde
        print '最终资金净值：', net_value[-1]

    def calcIndicator(self, net_value):
        rtn_daily = np.ones(len(net_value)) * np.nan
        rtn_daily[1:] = net_value[1:] / net_value[:-1] - 1.
        annual_rtn = np.nanmean(rtn_daily) * 250
        annual_std = np.nanstd(rtn_daily) * np.sqrt(250)
        sharpe = annual_rtn / annual_std
        # 最大回撤
        index_end = np.argmax(np.maximum.accumulate(net_value) - net_value)
        index_start = np.argmax(net_value[:index_end])
        max_drawdown = net_value[index_end] - net_value[index_start]
        # 最大回撤时间段
        max_drawdown_start = self.dt[index_start]
        max_drawdown_end = self.dt[index_end]

        return annual_rtn, annual_std, sharpe, max_drawdown, max_drawdown_start, max_drawdown_end



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
