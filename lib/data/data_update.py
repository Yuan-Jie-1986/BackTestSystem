#coding=utf-8

import yaml
from base import DataSaving
import sys


f = open('config.yaml')
res = yaml.load(f)
host = res['host']
port = res['port']
usr = res['user']
pwd = res['pwd']
db = res['db_name']

ds_obj = DataSaving(host=host, port=port, usr=usr, pwd=pwd, db=db)

cols = res['collection']
for col in cols:

    col_content = res[col]
    for cc in col_content:
        func = cc.pop('func')
        cmd_list = cc.pop('cmd')
        if isinstance(cmd_list, list):
            for c in cmd_list:
                getattr(ds_obj, func)(col, c, **cc)
        else:
            getattr(ds_obj, func)(col, cmd_list, **cc)

f.close()
