# coding=utf-8
import pandas as pd
from factool import FactorAnalysis as fa
import pymongo
import matplotlib.pyplot as plt
from datetime import datetime

conn = pymongo.MongoClient(host='192.168.1.172', port=27017)
db = conn['CBNB']
db.authenticate(name='yuanjie', password='yuanjie')
futures_coll = db['FuturesMD']
spot_coll = db['SpotMD']
edb_coll = db['EDB']

fut_spot_1 = {'L.DCE': 'LL神华煤化工价格',
              'PP.DCE': 'PP华东现货价',
              'MA.CZC': '甲醇华东（江苏地区）',
              'TA.CZC': 'TA内盘人民币价',
              'BU.SHF': '国产重交-山东',
              'V.DCE': '现货（常州sg-5低端价）',
              'EG.DCE': 'MEG'}

fut_spot_2 = {'ZC.CZC': '秦皇岛港:平仓价:动力末煤(Q5500):山西产',
              'JM.DCE': '天津港:库提价(含税):主焦煤(A<8%,V28%,0.8%S,G95,Y20mm):澳大利亚产',
              'RB.SHF': '价格:螺纹钢:HRB400 20mm:上海',
              'J.DCE': '天津港:平仓价(含税):一级冶金焦(A<12.5%,<0.65%S,CSR>65%,Mt8%):山西产',
              'I.DCE': '车板价:青岛港:澳大利亚:PB粉矿:61.5%',
              'HC.SHF': '价格:热轧板卷:Q235B:4.75mm:杭州'}

fut_spot_3 = {'LCOc1': 'LCOc2',
              'CLc1': 'CLc2',
              'RBc1': 'RBc2',
              'EBOBNWEMc1': 'EBOBNWEMc2',
              'NACFRJPSWMc1': 'NACFRJPSWMc2',
              'MOG92SGMc1': 'MOG92SGMc2',
              'HOc1': 'HOc2',
              'LGOc1': 'LGOc2',
              'GO10SGSWMc1': 'GO10SGSWMc2'}

jicha_df = pd.DataFrame()
cls_df = pd.DataFrame()

for f, s in fut_spot_1.items():

    queryArgs = {'wind_code': f, 'date': {'$gte': datetime(2013, 1, 1), '$lte': datetime(2019, 4, 4)}}
    projectionField = ['date', 'CLOSE']
    res = futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
    fut_df = pd.DataFrame.from_records(res, index='date')
    fut_df.rename(columns={'CLOSE': f}, inplace=True)
    fut_df.drop(columns=['_id'], inplace=True)
    cls_df = cls_df.join(fut_df, how='outer')

    queryArgs = {'commodity': s, 'date': {'$gte': datetime(2013, 1, 1), '$lte': datetime(2019, 4, 4)}}
    projectionField = ['date', 'price']
    res = spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
    spot_df = pd.DataFrame.from_records(res, index='date')
    spot_df.rename(columns={'price': f}, inplace=True)
    spot_df.drop(columns='_id', inplace=True)

    jicha = spot_df - fut_df
    jicha_df = jicha_df.join(jicha, how='outer')

for f, s in fut_spot_2.items():
    queryArgs = {'wind_code': f, 'date': {'$gte': datetime(2013, 1, 1), '$lte': datetime(2019, 4, 4)}}
    projectionField = ['date', 'CLOSE']
    res = futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
    fut_df = pd.DataFrame.from_records(res, index='date')
    fut_df.rename(columns={'CLOSE': f}, inplace=True)
    fut_df.drop(columns=['_id'], inplace=True)
    cls_df = cls_df.join(fut_df, how='outer')

    queryArgs = {'edb_name': s, 'date': {'$gte': datetime(2013, 1, 1), '$lte': datetime(2019, 4, 4)}}
    projectionField = ['date', 'CLOSE']
    res = spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
    spot_df = pd.DataFrame.from_records(res, index='date')
    spot_df.rename(columns={'CLOSE': f}, inplace=True)
    spot_df.drop(columns='_id', inplace=True)

    jicha = spot_df - fut_df
    jicha_df = jicha_df.join(jicha, how='outer')

# for f, s in fut_spot_3.items():
#     queryArgs = {'tr_code': f, 'date': {'$gte': datetime(2013, 1, 1), '$lte': datetime(2019, 4, 4)}}
#     projectionField = ['date', 'CLOSE']
#     res = futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
#     fut_df = pd.DataFrame.from_records(res, index='date')
#     fut_df.rename(columns={'CLOSE': f}, inplace=True)
#     fut_df.drop(columns=['_id'], inplace=True)
#     cls_df = cls_df.join(fut_df, how='outer')
#
#     queryArgs = {'tr_code': s, 'date': {'$gte': datetime(2013, 1, 1), '$lte': datetime(2019, 4, 4)}}
#     projectionField = ['date', 'CLOSE']
#     res = futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
#     spot_df = pd.DataFrame.from_records(res, index='date')
#     spot_df.rename(columns={'CLOSE': f}, inplace=True)
#     spot_df.drop(columns='_id', inplace=True)
#
#     jicha = spot_df - fut_df
#     jicha_df = jicha_df.join(jicha, how='outer')


cls_df = cls_df[jicha_df.columns]
a = fa(jicha_df, cls_df)

rank_ic = a.rank_ic(20)
normal_ic = a.normal_ic(20)
fac_rtn = a.fac_rtn()
print rank_ic.describe()
print normal_ic.describe()
print fac_rtn.describe()
rank_ic.plot()
normal_ic.plot()
fac_rtn.plot()
plt.show()


