# coding=utf-8
import numpy as np
import pandas as pd
import pymongo
import matplotlib.pyplot as plt
import matplotlib.dates as mdt

host = '192.168.1.172'
port = 27017
user = 'yuanjie'
passwd = 'yuanjie'

conn = pymongo.MongoClient(host=host, port=port)
db = conn['CBNB']
db.authenticate(name=user, password=passwd)
coll = db['FuturesMinMD']

ctr1 = 'HC.SHF'
ctr2 = 'RB.SHF'

queryArgs = {'wind_code': ctr1}
projectionField = ['wind_code', 'date_time', 'close', 'open', 'high', 'low', 'volume', 'position']
res = coll.find(queryArgs, projectionField)
df_1 = pd.DataFrame.from_records(res)
df_1.index = df_1['date_time']
df_1.drop(columns='_id', inplace=True)
df_1 = df_1[projectionField]
df_1.sort_index(ascending=True, inplace=True)
df_1['high-open'] = df_1['high'] - df_1['open']
df_1.dropna(inplace=True)


queryArgs = {'wind_code': ctr2}
projectionField = ['wind_code', 'date_time', 'close', 'open', 'high', 'low', 'volume', 'position']
res = coll.find(queryArgs, projectionField)
df_2 = pd.DataFrame.from_records(res)
df_2.index = df_2['date_time']
df_2.drop(columns='_id', inplace=True)
df_2 = df_2[projectionField]
df_2.sort_index(ascending=True, inplace=True)
df_2['high-open'] = df_2['high'] - df_2['open']
df_2.dropna(inplace=True)

df_res = pd.concat({ctr1: df_1['close'], ctr2: df_2['close']}, axis=1, join='outer')

# df_5min = df_res.resample('5T', label='right', closed='right').last()

df_res['ratio'] = df_res[ctr1] / df_res[ctr2]
df_res['diff'] = df_res[ctr1] - df_res[ctr2]
# print df_res.to_clipboard()
# print df_5min.to_clipboard()

ax = df_res[[ctr1, ctr2]] .plot(secondary_y=[ctr2], grid=True)
plt.figure()
df_res['diff'].plot(grid=True)
df_res.to_clipboard()
df_corr = df_res[ctr1].rolling(window=1000, min_periods=990).corr(df_res[ctr2])
plt.figure()
df_corr.plot()

# df_res.plot()
plt.show()
# print df_res









