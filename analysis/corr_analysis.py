# coding=utf-8

import pymongo
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


def mongo_login():
    # MongoDB的登录，返回的是登录的数据库CBNB
    host = 'localhost'
    port = 27017
    usr = 'yuanjie'
    pwd = 'yuanjie'
    db_name = 'CBNB'
    conn = pymongo.MongoClient(host=host, port=port)
    db = conn[db_name]
    db.authenticate(name=usr, password=pwd)
    return db


def getData(wind_code, field=['CLOSE']):
    #
    db = mongo_login()
    coll = db['FuturesMD']
    queryArgs = {'wind_code': wind_code}
    projectionField = ['date', 'wind_code'] + field
    searchRes = coll.find(queryArgs, projectionField).sort('date', pymongo.ASCENDING)
    df = pd.DataFrame.from_records(searchRes, index='date')
    df.drop(labels=['_id'], axis=1, inplace=True)
    return df

def calcCorrel(contract1, contract2, field1, field2):
    asset1 = getData(wind_code=contract1, field=field1)
    asset2 = getData(wind_code=contract2, field=field2)
    asset1.drop('wind_code', axis=1, inplace=True)
    asset2.drop('wind_code', axis=1, inplace=True)
    asset = asset1.join(asset2, how='outer', lsuffix='_%s' % contract1, rsuffix='_%s' % contract2)
    return asset






if __name__ == '__main__':
    # asset1 = getData('RB.SHF')
    # asset2 = getData('I.DCE')
    # asset1.drop('wind_code', axis=1, inplace=True)
    # asset2.drop('wind_code', axis=1, inplace=True)
    # asset = asset1.join(asset2, how='outer', lsuffix='_RB', rsuffix='_I')
    #
    #
    # print asset.corr().iloc[0,1]
    a = calcCorrel('L.DCE', 'TA.CZC', field1=['CLOSE'], field2=['CLOSE'])
    print a.corr()