#coding=utf-8
import pymongo
from WindPy import w
import pandas as pd
from datetime import datetime, date, timedelta
import pprint
import sys
import re
import eikon as ek
import logging


class DataSaving(object):
    def __init__(self, host, port, usr, pwd, db, log_path):

        self.conn = pymongo.MongoClient(host=host, port=port)
        self.db = self.conn[db]
        self.db.authenticate(usr, pwd)

        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

        formatter = logging.Formatter(fmt='%(asctime)s %(name)s %(filename)s %(funcName)s %(levelname)s %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S %a')

        fh = logging.FileHandler(log_path)
        ch = logging.StreamHandler()
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    @staticmethod
    def rtConn():
        TR_ID = '70650e2c881040408f6f95dea2bf3fa13e9f66fe'
        ek.set_app_key(TR_ID)
        return None

    @staticmethod
    def windConn():
        if not w.isconnected():
            w.start()
        return None

    def getFuturesInfoFromWind(self, collection, cmd, **kwargs):
        self.windConn()
        coll = self.db[collection]
        wres = w.wset(tablename='futurecc', startdate='1990-01-01', enddate=datetime.today(), wind_code=cmd)
        wfields = wres.Fields
        unit_total = len(wfields) * len(wres.Data[0])
        self.logger.info(u'共抓取了关于%s品种%d个单元格数据' % (cmd, unit_total))
        res = dict(zip(wfields, wres.Data))
        res.pop('change_limit')
        res.pop('target_margin')
        df = pd.DataFrame.from_dict(res)
        fu_info = df.to_dict(orient='index')
        for i, v in fu_info.items():
            v.update(kwargs)
            # 用来解决如果出现NaT的数据，无法传入数据库的问题
            if pd.isnull(v['last_delivery_month']):
                v['last_delivery_month'] = None
            if not coll.find_one({'wind_code': v['wind_code']}):
                v['update_time'] = datetime.now()
                coll.insert_one(v)

        return

    def getFuturePriceFromWind(self, collection, contract, alldaytrade, update=1, **kwargs):
        self.windConn()
        coll = self.db['Information']
        queryArgs = {'wind_code': contract}
        projectionField = ['wind_code', 'contract_issue_date', 'last_trade_date']
        searchRes = coll.find_one(queryArgs, projectionField)

        if not searchRes:
            # WIND主力合约, 通常结构为商品代码.交易所代码的形式
            if update == 0:
                # 一次性抓取全样本数据
                # 起始日期为该商品的所有合约中最早的contract_issue_date
                ptn1 = re.compile('[A-Z]+(?=\.)')
                cmd1 = ptn1.search(contract).group()
                ptn2 = re.compile('(?<=\.)[A-Z]+')
                cmd2 = ptn2.search(contract).group()
                queryArgs = {'wind_code': {'$regex': '\A%s.+%s\Z' % (cmd1, cmd2)}}
                projectionField = ['wind_code', 'contract_issue_date']
                searchRes = coll.find(queryArgs, projectionField).sort('contract_issue_date',
                                                                       pymongo.ASCENDING).limit(1)
                start_date = list(searchRes)[0]['contract_issue_date']
                coll = self.db[collection]
            elif update == 1:
                coll = self.db[collection]
                queryArgs = {'wind_code': contract}
                projectionField = ['wind_code', 'date']
                searchRes = coll.find(queryArgs, projectionField).sort('date', pymongo.DESCENDING).limit(1)
                start_date = list(searchRes)[0]['date'] + timedelta(1)

            if datetime.now().hour < 16 or alldaytrade:
                end_date = datetime.today() - timedelta(1)
            else:
                end_date = datetime.today()

        else:
            coll = self.db[collection]

            if update == 0:
                # 一次性全样本抓取数据
                # 起始日期为合约开始的日期
                # 当前时间还未收盘或者是全天交易的品种，则结束日期为前一天或合约最后交易日，否则为当天或合约最后交易日
                start_date = searchRes['contract_issue_date']
                if datetime.now().hour < 16 or alldaytrade:
                    end_date = min(datetime.today() - timedelta(1), searchRes['last_trade_date'])
                else:
                    end_date = min(datetime.today(), searchRes['last_trade_date'])
            elif update == 1:
                # 更新新的数据
                # 在数据库中查找到已有的数据的最后日期，然后+1作为起始日期
                # 结束日期同上
                queryArgs = {'wind_code': contract}
                projectionField = ['wind_code', 'date']
                mres = coll.find(queryArgs, projectionField).sort('date', pymongo.DESCENDING).limit(1)
                dt_l = list(mres)[0]['date']
                if dt_l >= searchRes['last_trade_date']:
                    return
                elif dt_l < searchRes['last_trade_date']:
                    start_date = dt_l + timedelta(1)
                    if datetime.now().hour < 16 or alldaytrade:
                        end_date = datetime.today() - timedelta(1)
                    else:
                        end_date = datetime.today()

        if start_date > end_date:
            return

        res = w.wsd(contract, 'open, high, low, close, volume, amt, dealnum, oi, settle',
                    beginTime=start_date, endTime=end_date)

        if res.ErrorCode == -40520007:
            self.logger.info(u'WIND提取%s到%s的%s数据出现了错误' % (start_date, end_date, contract))
            return
        elif res.ErrorCode != 0:
            print res
            raise Exception(u'WIND提取数据出现了错误')
        else:
            unit_total = len(res.Data[0]) * len(res.Fields)
            self.logger.info(u'抓取%s合约%s到%s的市场价格数据，共计%d个' % (contract, start_date, end_date, unit_total))
            dict_res = dict(zip(res.Fields, res.Data))
            df = pd.DataFrame.from_dict(dict_res)
            df.index = res.Times
            df['wind_code'] = contract
            price_dict = df.to_dict(orient='index')
            total = len(price_dict)
            count = 1
            print u'抓取%s合约的数据' % contract
            for di in price_dict:
                process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
                sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
                sys.stdout.flush()

                dtemp = price_dict[di].copy()
                dtemp['date'] = datetime.strptime(str(di), '%Y-%m-%d')
                dtemp['update_time'] = datetime.now()
                dtemp.update(kwargs)
                # print kwargs
                # print dtemp
                # print len(dtemp)

                coll.insert_one(dtemp)
                count += 1

            sys.stdout.write('\n')
            sys.stdout.flush()

    def getFutureGroupPriceFromWind(self, collection, cmd, **kwargs):

        coll = self.db['Information']

        # 在Information表中查找与该商品有关的所有合约
        # 只针对国内的商品合约
        # 查找之后还需要填加主力合约的代码

        ptn1 = re.compile('[A-Z]+(?=\.)')
        cmd1 = ptn1.search(cmd).group()
        ptn2 = re.compile('(?<=\.)[A-Z]+')
        cmd2 = ptn2.search(cmd).group()

        queryArgs = {'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (cmd1, cmd2)}}
        projectionField = ['wind_code']
        searchRes = coll.find(queryArgs, projectionField)

        contract_list = [s['wind_code'] for s in searchRes]
        # 增加主力合约代码
        contract_list.append(cmd)

        coll = self.db[collection]
        for d in contract_list:
            if coll.find_one({'wind_code': d}):
                self.getFuturePriceFromWind(collection=collection, contract=d, update=1, **kwargs)
            else:
                self.getFuturePriceFromWind(collection=collection, contract=d, update=0, **kwargs)

    def getEDBFromWind(self, collection, edb_code, **kwargs):
        self.windConn()
        coll = self.db[collection]
        if coll.find_one({'wind_code': edb_code}):
            queryArgs = {'wind_code': edb_code}
            projectionField = ['wind_code', 'date']
            searchRes = coll.find(queryArgs, projectionField).sort('date', pymongo.DESCENDING).limit(1)
            start_date = list(searchRes)[0]['date'] + timedelta(1)
            end_date = datetime.today()
        else:
            start_date = datetime.strptime('19900101', '%Y%m%d')
            end_date = datetime.today()

        if start_date > end_date:
            return
        res = w.edb(edb_code, start_date, end_date, 'Fill=previous')

        if res.ErrorCode != 0:
            print res
            raise Exception(u'WIND提取数据出现了错误')
        else:
            unit_total = len(res.Data[0]) * len(res.Fields)
            self.logger.info(u'抓取EDB%s数据%s到%s的数据，共计%d个' % (edb_code, start_date, end_date, unit_total))
            dict_res = dict(zip(res.Fields, res.Data))
            df = pd.DataFrame.from_dict(dict_res)
            df.index = res.Times
            df['wind_code'] = edb_code
            df2dict = df.to_dict(orient='index')

            total = len(df2dict)
            count = 1
            print '抓取%s数据' % edb_code
            for di in df2dict:
                process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
                sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
                sys.stdout.flush()

                # 该判断是必要的，因为如果日期是之后的，而数据没有，edb方法会返回最后一个数据
                if coll.find_one({'wind_code': edb_code, 'date': datetime.strptime(str(di), '%Y-%m-%d')}):
                    self.logger.info(u'该数据已经存在于数据库中，没有抓取')
                    continue

                dtemp = df2dict[di].copy()
                dtemp['date'] = datetime.strptime(str(di), '%Y-%m-%d')
                dtemp['update_time'] = datetime.now()
                dtemp.update(kwargs)
                coll.insert_one(dtemp)
                count += 1

            sys.stdout.write('\n')
            sys.stdout.flush()

    def getPriceFromRT(self, collection, cmd, type, **kwargs):
        """
        futures是来判断是否抓取期货数据，涉及到字段问题
        这里的一个非常重要的问题就是交易时间
        比如现在北京时间凌晨1点，欧美交易所的时间仍是昨天，此时如果抓取数据，虽然是抓昨天的数据，但是交易依然在进行，所以此时会出错
        """

        if not ek.get_app_key():
            self.rtConn()
        coll = self.db[collection]

        if coll.find_one({'tr_code': cmd}):
            queryArgs = {'tr_code': cmd}
            projectionField = ['tr_code', 'date']
            searchRes = coll.find(queryArgs, projectionField).sort('date', pymongo.DESCENDING).limit(1)
            start_date = list(searchRes)[0]['date'] + timedelta(1)
            end_date = datetime.today() - timedelta(1)
        else:
            start_date = datetime.strptime('2000-01-01', '%Y-%m-%d')
            end_date = datetime.today() - timedelta(1)

        if start_date > end_date:
            return

        if type == 'futures':
            fields = ['HIGH', 'LOW', 'OPEN', 'CLOSE', 'VOLUME']
        elif type == 'swap' or type == 'spot':
            fields = ['CLOSE']

        res = ek.get_timeseries(cmd, start_date=start_date, end_date=end_date, fields=fields)

        if 'COUNT' in res.columns:
            self.logger.info(u'抓取%s%s到%s数据失败，行情交易未结束，请稍后重试' % (cmd, start_date, end_date))
            return

        unit_total = len(res.values.flatten())
        self.logger.info(u'抓取%s%s到%s的数据，共计%d个' % (cmd, start_date, end_date, unit_total))

        res['tr_code'] = cmd
        res_dict = res.to_dict(orient='index')

        total = len(res_dict)
        count = 1
        print u'抓取路透%s合约的数据' % cmd
        for di in res_dict:
            process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
            sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
            sys.stdout.flush()

            dtemp = res_dict[di].copy()
            dtemp['date'] = di
            dtemp['update_time'] = datetime.now()
            dtemp.update(kwargs)

            coll.insert_one(dtemp)
            count += 1

        sys.stdout.write('\n')
        sys.stdout.flush()

        return

    def getDataFromCSV(self, collection, cmd, path, **kwargs):
        """
        从csv文件中导入数据到数据库
        """
        coll = self.db[collection]
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        df = df.astype('float64')  # 将数据转成浮点型，否则存入数据库中会以NumberLong的数据类型

        if coll.find_one({'commodity': cmd}):
            searchRes = coll.find({'commodity': cmd}, ['date']).sort('date', pymongo.DESCENDING).limit(1)
            start_date = list(searchRes)[0]['date']
            df = df[df.index > start_date]
        else:
            start_date = df.index[0]

        if df.empty:
            return

        unit_total = len(df.values.flatten())
        self.logger.info(u'抓取%s%s之后的数据，共计%d个' % (cmd, start_date, unit_total))

        # 关于编码的问题，如果是中文，需要将unicode转成str
        df.rename(columns={cmd.encode('utf-8'): 'price'}, inplace=True)

        df['commodity'] = cmd
        for k, v in kwargs.items():
            df[k] = v
        res_dict = df.to_dict(orient='index')
        total = len(res_dict)
        count = 1
        print u'抓取%s数据' % cmd
        for di in res_dict:
            process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
            sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
            sys.stdout.flush()
            dtemp = res_dict[di].copy()
            dtemp['date'] = di
            dtemp['update_time'] = datetime.now()
            coll.insert_one(dtemp)

            count += 1

        sys.stdout.write('\n')
        sys.stdout.flush()


    def getDateSeries(self, collection, cmd, **kwargs):
        """从WIND导入交易日期时间序列"""
        self.windConn()
        coll = self.db[collection]
        if coll.find_one({'exchange': cmd}):
            queryArgs = {'exchange': cmd}
            projectionField = ['date']
            searchRes = coll.find(queryArgs, projectionField).sort('date', pymongo.DESCENDING).limit(1)
            start_date = list(searchRes)[0]['date'] + timedelta(1)
            end_date = datetime.today()
        else:
            start_date = datetime.strptime('2000-01-01', '%Y-%m-%d')
            end_date = datetime.today()

        if start_date > end_date:
            return

        if cmd == 'SHSE':
            res = w.tdays(beginTime=start_date, endTime=end_date)
        else:
            res = w.tdays(beginTime=start_date, endTime=end_date, TradingCalendar=cmd)

        total = len(res.Data[0])
        count = 1

        print u'更新交易日期数据'
        self.logger.info(u'共更新了%s个交易日期数据进入到数据库' % total)
        for r in res.Data[0]:
            process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
            sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
            sys.stdout.flush()
            res_dict = {'date': r, 'exchange': cmd, 'update_time': datetime.now()}
            res_dict.update(kwargs)
            coll.insert_one(res_dict)
            count += 1

        sys.stdout.write('\n')
        sys.stdout.flush()

    def combineMainContract(self, source_collection, target_collection, cmd, method, ):
        source = self.db[source_collection]
        target = self.db[target_collection]
        info_source = self.db['Information']
        queryArgs = {'code': {'$regex': cmd}}

        res = info_source.find(queryArgs)
        for r in res:
            print r


if __name__ == '__main__':
    # DataSaving().getFuturesPriceAuto(security='M')
    # DataSaving().getFuturesInfoFromWind('BU.SHF')

    # DataSaving().getFuturePriceAutoFromWind('TA')

    # DataSaving().getMainFuturePriceAutoFromWind(cmd='B.IPE', alldaytrade=1)
    # DataSaving().getAllFuturesInfoFromWind()
    # DataSaving().test('J.DCE')
    # DataSaving().getFXFromWind('即期汇率:美元兑人民币')
    # DataSaving().getFuturePriceFromRT('LCO')
    a = DataSaving(host='192.168.1.172', port=27017, usr='yuanjie', pwd='yuanjie', db='CBNB',
                   log_path="E:\\CBNB\\BackTestSystem\\data_saving.log")
    # a.getFuturesInfoFromWind(collection='Information', cmd='BU.SHF')
    # a.getFuturePriceFromWind('FuturesMD', 'TA.CZC', alldaytrade=0)
    # a.getPriceFromRT('FuturesMD', cmd='LCOc1', type='futures')
    # a.getDataFromCSV(collection='SpotMD', cmd='PX', path='E:\\CBNB\\BackTestSystem\\lib\\data\\supplement_db\\PX.csv')
    # res = w.wset(tablename='futurecc', startdate='2018-01-01', enddate='2018-10-19', wind_code='TA.CZC')
    # print res

    # a.getDateSeries(collection='DateDB', cmd='SHSE', frequecy='Daily')
    a.combineMainContract(source_collection='FuturesMD', target_collection='DerivDB', cmd='TA', method='1MonPrevious')


