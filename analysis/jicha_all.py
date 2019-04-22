#coding=utf-8

import pymongo
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pyecharts as pec

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

                # 'FU.SHF': 'FO380-SIN'}

ctr_pairs = {'LCOc1': 'LCOc2',
             'CLc1': 'CLc2',
             'RBc1': 'RBc2',
             'EBOBNWEMc1': 'EBOBNWEMc2',
             'NACFRJPSWMc1': 'NACFRJPSWMc2',
             'MOG92SGMc1': 'MOG92SGMc2',
             'HOc1': 'HOc2',
             'LGOc1': 'LGOc2',
             'GO10SGSWMc1': 'GO10SGSWMc2'}

jicha_total = pd.DataFrame()
jicha_1 = pd.DataFrame()
jicha_2 = pd.DataFrame()
jicha_3 = pd.DataFrame()

for k in fut_spot_1:
    # if k == 'FU.SHF':
    queryArgs = {'wind_code': k}
    projectionField = ['date', 'CLOSE']
    res = futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
    fut_df = pd.DataFrame.from_records(res, index='date')
    fut_df.rename(columns={'CLOSE': k}, inplace=True)
    fut_df.drop(columns=['_id'], inplace=True)

    queryArgs = {'commodity': fut_spot_1[k]}
    projectionField = ['date', 'price']
    res = spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
    spot_df = pd.DataFrame.from_records(res, index='date')
    spot_df.rename(columns={'price': fut_spot_1[k]}, inplace=True)
    spot_df.drop(columns=['_id'], inplace=True)

    jicha_df = fut_df.join(spot_df, how='outer')
    jicha_df[k] = 1 - jicha_df[k] / jicha_df[fut_spot_1[k]]
    jicha_df.dropna(inplace=True)

    jicha_1 = jicha_1.join(jicha_df[k], how='outer')
    jicha_total = jicha_total.join(jicha_df[k], how='outer')

for k in fut_spot_2:
    # if k == 'FU.SHF':
    queryArgs = {'wind_code': k}
    projectionField = ['date', 'CLOSE']
    res = futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
    fut_df = pd.DataFrame.from_records(res, index='date')
    fut_df.rename(columns={'CLOSE': k}, inplace=True)
    fut_df.drop(columns=['_id'], inplace=True)

    queryArgs = {'edb_name': fut_spot_2[k]}
    projectionField = ['date', 'CLOSE']
    res = spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
    spot_df = pd.DataFrame.from_records(res, index='date')
    spot_df.rename(columns={'CLOSE': fut_spot_2[k]}, inplace=True)
    spot_df.drop(columns=['_id'], inplace=True)

    jicha_df = fut_df.join(spot_df, how='outer')
    jicha_df[k] = 1 - jicha_df[k] / jicha_df[fut_spot_2[k]]
    jicha_df.dropna(inplace=True)

    jicha_2 = jicha_2.join(jicha_df[k], how='outer')
    jicha_total = jicha_total.join(jicha_df[k], how='outer')

for k in ctr_pairs:
    # if k == 'FU.SHF':
    queryArgs = {'tr_code': k}
    projectionField = ['date', 'CLOSE']
    res = futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
    fut_df = pd.DataFrame.from_records(res, index='date')
    fut_df.rename(columns={'CLOSE': k}, inplace=True)
    fut_df.drop(columns=['_id'], inplace=True)

    queryArgs = {'tr_code': ctr_pairs[k]}
    projectionField = ['date', 'CLOSE']
    res = futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
    spot_df = pd.DataFrame.from_records(res, index='date')
    spot_df.rename(columns={'CLOSE': ctr_pairs[k]}, inplace=True)
    spot_df.drop(columns=['_id'], inplace=True)

    jicha_df = fut_df.join(spot_df, how='outer')
    jicha_df[k] = 1 - jicha_df[k] / jicha_df[ctr_pairs[k]]
    jicha_df.dropna(inplace=True)

    jicha_3 = jicha_3.join(jicha_df[k], how='outer')
    jicha_total = jicha_total.join(jicha_df[k], how='outer')


jicha_1.plot(grid=True)

jicha_2.plot(grid=True)

jicha_3.plot(grid=True)

jicha_total.plot(grid=True)
plt.show()

# queryArgs = {'wind_code': 'M0067855'}
# projectionField = ['date', 'CLOSE']
# res = edb_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
# exchange_df = pd.DataFrame.from_records(res, index='date')
# exchange_df.rename(columns={'CLOSE': 'exchange'}, inplace=True)
# exchange_df.drop(columns=['_id'], inplace=True)

#=================================================================================================


# queryArgs = {'tr_code': fut_ctr}
# projectionField = ['date', 'CLOSE']
# res = futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
# fut_df = pd.DataFrame.from_records(res, index='date')
# fut_df.rename(columns={'CLOSE': fut_ctr}, inplace=True)
# fut_df.drop(columns=['_id'], inplace=True)

#=================================================================================================



# queryArgs = {'tr_code': fut_spot[fut_ctr]}
# projectionField = ['date', 'CLOSE']
# res = spot_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
# spot_df = pd.DataFrame.from_records(res, index='date')
# spot_df.rename(columns={'CLOSE': fut_spot[fut_ctr]}, inplace=True)
# spot_df.drop(columns=['_id'], inplace=True)

# queryArgs = {'tr_code': ctr_pairs[fut_ctr]}
# projectionField = ['date', 'CLOSE']
# res = futures_coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
# spot_df = pd.DataFrame.from_records(res, index='date')
# spot_df.rename(columns={'CLOSE': ctr_pairs[fut_ctr]}, inplace=True)
# spot_df.drop(columns=['_id'], inplace=True)
#=================================================================================================

# spot_df = spot_df.join(exchange_df)
# spot_df[fut_spot[fut_ctr]] = spot_df[fut_spot[fut_ctr]] * spot_df['exchange']
# spot_df.drop(columns=['exchange'], inplace=True)


#=================================================================================================

# jicha_df = fut_df.join(spot_df, how='outer')
# jicha_df['jicha'] = 1 - jicha_df[fut_ctr] / jicha_df[fut_spot[fut_ctr]]
# jicha_df.dropna(inplace=True)
# jicha_df.plot(secondary_y=['jicha'], grid=True)
# print jicha_df['jicha'].describe()
#
# jicha_df['year'] = [i.year for i in jicha_df.index]
# year_list = sorted(list(set([i.year for i in jicha_df.index])))
#
# for y in year_list:
#     print y
#     print jicha_df[jicha_df['year'] == y]['jicha'].describe()
#
#
# plt.show()
