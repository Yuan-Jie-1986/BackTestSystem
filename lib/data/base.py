#coding=utf-8
import pymongo
from WindPy import w
import pandas as pd
from jqdatasdk import *
from datetime import datetime, date, timedelta
import pprint
import sys
from constant import *
import re


class DataSaving(object):
    def __init__(self, host='localhost', port=27017):

        if not w.isconnected():
            w.start()
        auth('13585598782', '598782')
        self.conn = pymongo.MongoClient(host=host, port=port)

        self.cmd_exchange = self.getCmdExchange()
        print self.cmd_exchange

    def getCmdExchange(self):
        ptn1 = re.compile('[A-Z]+(?=\.)')
        ptn2 = re.compile('(?<=\.)[A-Z]+')
        cmd_dict = {}
        for f in futures_type:
            cmd = ptn1.search(f).group()
            # exchange = ptn2.search(f).group()
            cmd_dict[cmd] = f
            # cmd_dict[cmd] = exchange
        return cmd_dict

    def useMongoDB(self, db):

        self.db = self.conn[db]

    def useMongoCollections(self, collection):
        if self.db:
            self.collection = self.db[collection]
        else:
            raise Exception(u'请先选择数据库')

    def getFuturesInfoFromJQ(self):
        self.useMongoDB('FuturesDaily')
        self.useMongoCollections('SecurityInfoFromJQ')
        sec_info = get_all_securities(types=['futures'])
        secinfo_dict = sec_info.to_dict('index')

        total = len(secinfo_dict)
        count = 1


        for s in secinfo_dict:

            process_str = '>' * int(count * 100. / total) + ' ' * int(100. - count * 100. / total)

            sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
            sys.stdout.flush()


            d = secinfo_dict[s]
            d['jq_code'] = s
            if not self.collection.find_one({'jq_code': s}):
                self.collection.insert_one(d)

            count += 1

        sys.stdout.write('\n')
        sys.stdout.flush()

    def getFuturesPrice(self, security, security_class, update=1):
        self.useMongoDB('FuturesDaily')
        self.useMongoCollections('SecurityInfoFromJQ')
        queryArgs = {'jq_code': security}
        projectionFields = ['jq_code', 'start_date', 'end_date']
        searchRes = self.collection.find_one(queryArgs, projectionFields)

        self.useMongoCollections('%s_Daily' % security_class)
        if update == 0:

            if datetime.now().hour < 16:
                end_date = min(datetime.today() - timedelta(1), searchRes['end_date'])
            else:
                end_date = min(datetime.today(), searchRes['end_date'])
            df_price = get_price(searchRes['jq_code'], start_date=searchRes['start_date'], end_date=end_date)

            df_sett = get_extras(info='futures_sett_price', security_list=searchRes['jq_code'],
                                 start_date=searchRes['start_date'], end_date=end_date)


            df_sett.rename(columns={searchRes['jq_code']: 'settle'}, inplace=True)
            df_oi = get_extras(info='futures_positions', security_list=searchRes['jq_code'],
                               start_date=searchRes['start_date'], end_date=end_date)
            df_oi.rename(columns={searchRes['jq_code']: 'openint'}, inplace=True)

            df = df_price.join(df_sett, how='outer')
            df = df.join(df_oi, how='outer')
            df_dict = df.to_dict('index')

            total = len(df_dict)

            count = 1.
            print u'导入合约%s' % searchRes['jq_code']

            for d in df_dict:
                process_str = '>' * int(count * 100. / total) + ' ' * int(100. - count * 100. / total)
                sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
                sys.stdout.flush()

                dn = df_dict[d]
                dn['jq_code'] = searchRes['jq_code']
                dn['date'] = d

                if not self.collection.find_one({'date': d, 'jq_code': searchRes['jq_code']}):
                    self.collection.insert_one(dn)

                count += 1

            sys.stdout.write('\n')
            sys.stdout.flush()

        if update == 1:
            if searchRes['end_date'] >= datetime.today():
                searchRes1 = self.collection.find({'jq_code': searchRes['jq_code']}, ['date', 'jq_code']). \
                    sort('date', pymongo.DESCENDING).limit(1)

                for s1 in searchRes1:

                    start_date = s1['date'] + timedelta(1)

                    if datetime.now().hour < 16:
                        end_date = datetime.today() - timedelta(1)
                    else:
                        end_date = datetime.today()

                    if start_date > end_date:
                        continue
                    else:
                        df_price = get_price(searchRes['jq_code'], start_date=start_date, end_date=end_date)
                        df_sett = get_extras(info='futures_sett_price', security_list=searchRes['jq_code'],
                                             start_date=start_date, end_date=end_date)
                        df_sett.rename(columns={searchRes['jq_code']: 'settle'}, inplace=True)
                        df_oi = get_extras(info='futures_positions', security_list=searchRes['jq_code'],
                                           start_date=start_date,
                                           end_date=end_date)
                        df_oi.rename(columns={searchRes['jq_code']: 'openint'}, inplace=True)

                        df = df_price.join(df_sett, how='outer')
                        df = df.join(df_oi, how='outer')
                        df_dict = df.to_dict('index')

                        total = len(df_dict)

                        count = 1.
                        print u'更新合约%s' % searchRes['jq_code']

                        for d in df_dict:
                            process_str = '>' * int(count * 100. / total) + ' ' * int(100. - count * 100. / total)
                            sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
                            sys.stdout.flush()

                            dn = df_dict[d]
                            dn['jq_code'] = searchRes['jq_code']
                            dn['date'] = d

                            if not self.collection.find_one({'date': d, 'jq_code': searchRes['jq_code']}):
                                self.collection.insert_one(dn)

                            count += 1

                        sys.stdout.write('\n')
                        sys.stdout.flush()

    def getFuturesPriceAuto(self, security):

        self.useMongoDB('FuturesDaily')
        self.useMongoCollections('SecurityInfoFromJQ')
        queryArgs = {'jq_code': {'$regex': '\A%s(?=\d+)' % security}}
        projectionFields = ['jq_code', 'start_date', 'end_date']
        searchRes = self.collection.find(queryArgs, projectionFields).sort('jq_code', pymongo.DESCENDING)

        self.useMongoCollections('%s_Daily' % security)


        for s in searchRes:

            if self.collection.find_one({'jq_code': s['jq_code']}):
                self.getFuturesPrice(security=s['jq_code'], security_class=security, update=1)
            else:
                self.getFuturesPrice(security=s['jq_code'], security_class=security, update=0)


    def getFuturesInfoFromWind(self, cmd):

        self.useMongoDB('FuturesDailyWind')
        self.useMongoCollections('FuturesInfo')
        wres = w.wset(tablename='futurecc', startdate='1990-01-01', enddate=datetime.today(), wind_code=cmd)
        wfields = wres.Fields
        res = dict(zip(wfields, wres.Data))
        df = pd.DataFrame.from_dict(res)
        df.index = wres.Data[wfields.index('wind_code')]
        fu_info = df.to_dict(orient='index')

        for i in fu_info:

            if not self.collection.find_one({'wind_code': i}):
                fu_info[i]['update_time'] = datetime.now()
                self.collection.insert_one(fu_info[i])
            else:
                one_res = self.collection.find_one({'wind_code': i}, {'change_limit': 1, 'target_margin': 1})
                change_limit = one_res['change_limit']
                target_margin = one_res['target_margin']

                if change_limit != fu_info[i]['change_limit'] or target_margin != fu_info[i]['target_margin']:
                    fu_info[i]['update_time'] = datetime.now()
                    self.collection.update_one({'wind_code': i}, {'$set': fu_info[i]})

    def getAllFuturesInfoFromWind(self):

        total = len(futures_type)
        count = 1
        for f in futures_type:
            process_str = '>' * int(count * 100. / total) + ' ' * int(100. - count * 100. / total)
            sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
            sys.stdout.flush()

            self.getFuturesInfoFromWind(f)

            count += 1

    def getFuturePriceFromWind(self, contract, cmd, update=1, alldaytrade=0):
        self.useMongoDB('FuturesDailyWind')
        self.useMongoCollections('FuturesInfo')
        queryArgs = {'wind_code': contract}
        projectionField = ['wind_code', 'contract_issue_date', 'last_trade_date']
        searchRes = self.collection.find_one(queryArgs, projectionField)


        self.useMongoCollections('%s_Daily' % cmd)

        if update == 0:
            start_date = searchRes['contract_issue_date']
            if datetime.now().hour < 16 or alldaytrade:
                end_date = min(datetime.today() - timedelta(1), searchRes['last_trade_date'])
            else:
                end_date = min(datetime.today(), searchRes['last_trade_date'])

        elif update == 1:
            queryArgs = {'wind_code': contract}
            projectionField = ['wind_code', 'date']
            mres = self.collection.find(queryArgs, projectionField).sort('date', pymongo.DESCENDING).limit(1)
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
        if res.ErrorCode != 0:
            print res
            raise Exception(u'WIND提取数据出现了错误')
        else:
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
                self.collection.insert_one(dtemp)
                count += 1

            sys.stdout.write('\n')
            sys.stdout.flush()

    def getMainFuturePriceFromWind(self, cmd, update=1, alldaytrade=1):
        self.useMongoDB('FuturesDailyWind')

        ptn1 = re.compile('[A-Z]+(?=\.)')
        cmd1 = ptn1.search(cmd).group()
        ptn2 = re.compile('(?<=\.)[A-Z]+')
        cmd2 = ptn2.search(cmd).group()

        if update == 0:
            self.useMongoCollections('FuturesInfo')
            queryArgs = {'wind_code': {'$regex': '\A%s.+%s\Z' % (cmd1, cmd2)}}
            projectionField = ['wind_code', 'contract_issue_date', 'last_trade_date']
            searchRes = self.collection.find(queryArgs, projectionField).sort('contract_issue_date', pymongo.ASCENDING).limit(1)
            start_date = list(searchRes)[0]['contract_issue_date']
            self.useMongoCollections('%s_Daily' % cmd)
        elif update == 1:
            self.useMongoCollections('%s_Daily' % cmd)
            queryArgs = {'wind_code': cmd}
            projectionField = ['wind_code', 'date']
            searchRes = self.collection.find(queryArgs, projectionField).sort('date', pymongo.DESCENDING).limit(1)
            start_date = list(searchRes)[0]['date'] + timedelta(1)
        else:
            raise Exception(u'参数输入错误')

        if datetime.now().hour < 16 or alldaytrade:
            end_date = datetime.today() - timedelta(1)
        else:
            end_date = datetime.today()

        if start_date > end_date:
            return

        res = w.wsd(cmd, 'open, high, low, close, volume, amt, dealnum, oi, settle',
                    beginTime=start_date, endTime=end_date)

        if res.ErrorCode != 0:
            print res
            raise Exception(u'WIND提取数据出现了错误')
        else:
            dict_res = dict(zip(res.Fields, res.Data))
            df = pd.DataFrame.from_dict(dict_res)
            df.index = res.Times
            df['wind_code'] = cmd
            price_dict = df.to_dict(orient='index')
            total = len(price_dict)
            count = 1
            print u'抓取%s合约的数据' % cmd
            for di in price_dict:
                process_str = '>' * int(count * 100. / total) + ' ' * (100 - int(count * 100. / total))
                sys.stdout.write('\r' + process_str + u'【已完成%5.2f%%】' % (count * 100. / total))
                sys.stdout.flush()

                dtemp = price_dict[di].copy()
                dtemp['date'] = datetime.strptime(str(di), '%Y-%m-%d')
                dtemp['update_time'] = datetime.now()
                self.collection.insert_one(dtemp)
                count += 1

            sys.stdout.write('\n')
            sys.stdout.flush()

    def getMainFuturePriceAutoFromWind(self, cmd, alldaytrade):
        self.useMongoDB('FuturesDailyWind')
        self.useMongoCollections('%s_Daily' % cmd)

        if self.collection.find_one({'wind_code': cmd}):
            self.getMainFuturePriceFromWind(cmd, 1, alldaytrade)
        else:
            self.getMainFuturePriceFromWind(cmd, 0, alldaytrade)

    def getFuturePriceAutoFromWind(self, cmd, alldaytrade):
        self.useMongoDB('FuturesDailyWind')
        self.useMongoCollections('FuturesInfo')

        ptn1 = re.compile('[A-Z]+(?=\.)')
        cmd1 = ptn1.search(cmd).group()
        ptn2 = re.compile('(?<=\.)[A-Z]+')
        cmd2 = ptn2.search(cmd).group()

        queryArgs = {'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (cmd1, cmd2)}}
        projectionField = ['wind_code', 'contract_issue_date', 'last_trade_date']
        searchRes = self.collection.find(queryArgs, projectionField).sort('wind_code', pymongo.ASCENDING)

        self.useMongoCollections('%s_Daily' % cmd)

        contract_dict = {}

        for s in searchRes:
            contract_dict[s['wind_code']] = {'start_date': s['contract_issue_date'],
                                             'end_date': s['last_trade_date']}

        for d in contract_dict:

            if self.collection.find_one({'wind_code': d}):
                self.getFuturePriceFromWind(contract=d, cmd=cmd, update=1, alldaytrade=alldaytrade)
            else:
                self.getFuturePriceFromWind(contract=d, cmd=cmd, update=0, alldaytrade=alldaytrade)

    def test(self, cmd):
        self.useMongoDB('FuturesDailyWind')
        self.useMongoCollections('FuturesInfo')

        ptn1 = re.compile('[A-Z]+(?=\.)')
        cmd1 = ptn1.search(cmd).group()
        ptn2 = re.compile('(?<=\.)[A-Z]+')
        cmd2 = ptn2.search(cmd).group()

        queryArgs = {'wind_code': {'$regex': '\A%s\d+\.%s\Z' % (cmd1, cmd2)}}
        projectionField = ['wind_code', 'contract_issue_date', 'last_trade_date']
        searchRes = self.collection.find(queryArgs, projectionField).sort('wind_code', pymongo.ASCENDING)
        for s in searchRes:
            print s


if __name__ == '__main__':
    # DataSaving().getFuturesPriceAuto(security='M')
    # DataSaving().getFuturesInfoFromWind('BU.SHF')

    # DataSaving().getFuturePriceAutoFromWind('TA')

    # DataSaving().getMainFuturePriceAutoFromWind(cmd='B.IPE', alldaytrade=1)
    # DataSaving().getAllFuturesInfoFromWind()
    DataSaving().test('J.DCE')
