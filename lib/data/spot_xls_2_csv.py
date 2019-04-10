#coding=utf-8
import pandas as pd
import numpy as np

path = 'E:\CBNB\BackTestSystem\lib\data\supplement_db'
file_nm = u'E:\CBNB\BackTestSystem\lib\data\supplement_db\现货价格.xlsx'


col_dict = {u'LL神华煤化工价格': 'LL',
            u'PP华东现货价': 'PP',
            u'甲醇华东（江苏地区）': 'MA',
            u'现货（常州sg-5低端价）': 'PVC',
            u'TA内盘人民币价': 'PTA',
            u'国产重交-山东': u'沥青',
            u'MEG': 'MEG'}

spot_xls = pd.read_excel(file_nm, index_col=u'日期')
spot_xls = spot_xls[col_dict.keys()]
for c in spot_xls.columns:
    spot_c = spot_xls[[c]]
    spot_c.replace(0, np.nan, inplace=True)
    spot_c.dropna(inplace=True)
    spot_c.to_csv(path + '\\' + col_dict[c] + '.csv', encoding='utf-8')