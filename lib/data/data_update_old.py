# coding=utf-8

# import inspect
# current_file = inspect.getabsfile(inspect.currentframe())
from base import DataSaving
import yaml


ins = DataSaving()

f = open('config.yaml')
yaml_content = yaml.load(f)
fut_wind = yaml_content['WIND']['FUTURES_MD']

for fut in fut_wind:
    exchange = fut['Exchange']
    trade_type = fut['Alldaytrade']
    commodity_list = fut['Commodity']
    for c in commodity_list:
        wind_code = '.'.join([c, exchange])
        ins.getFuturePriceAutoFromWind(cmd=wind_code, alldaytrade=trade_type)
        ins.getMainFuturePriceAutoFromWind(cmd=wind_code, alldaytrade=trade_type)



# Target = ['AP', 'RB', 'I', 'JM', 'J', 'L', 'MA', 'PP', 'RM', 'SC', 'TA']


# ins.getFuturesInfoFromJQ()
# for t in Target:
#     print t
#     ins.getFuturesPriceAuto(security=t)

# WIND_TARGET = ['TA.CZC', 'L.DCE', 'PP.DCE', 'MA.CZC', 'RB.SHF', 'I.DCE', 'J.DCE', 'JM.DCE', 'BU.SHF',
#                'RU.SHF', 'M.DCE', 'NI.SHF', 'C.DCE', 'SR.CZC', 'RM.CZC', 'HC.SHF', 'AP.CZC', 'ZC.CZC']
# for w in WIND_TARGET:
#     print w
#     ins.getFuturePriceAutoFromWind(cmd=w, alldaytrade=0)
#     ins.getMainFuturePriceAutoFromWind(cmd=w, alldaytrade=0)
#
# OIL_TARGET = ['B.IPE']
# for w in OIL_TARGET:
#     print w
#     ins.getFuturePriceAutoFromWind(cmd=w, alldaytrade=1)
#     ins.getMainFuturePriceAutoFromWind(cmd=w, alldaytrade=1)
#
# EDB_TARGET = ['即期汇率:美元兑人民币']
# for w in EDB_TARGET:
#     print w
#     ins.getFXFromWind(w)
#
# RT_TARGET = ['LCOc1', 'RBc1', 'HOc1', 'FO180SGSWMc1', 'FO380SGSWMc1', 'WTM-', 'WTC-']
# for w in RT_TARGET:
#     print w
#     ins.getPriceFromRT(w)
