# coding=utf-8
import pandas as pd
import numpy as np
import os
import paramiko

client = paramiko.SSHClient()
client.connect('192.168.1.17', 22, 'yuanjie', 'yuanjie')


address = u'192.168.1.17\\中基能化\\研究\\聚烯烃\\1-数据库\\1-1基础数据库\\聚烯烃供需数据库.xlsx'
print pd.read_excel(address)
