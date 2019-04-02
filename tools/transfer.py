# coding=utf-8
import paramiko
import os
import pandas as pd
from datetime import datetime


host = '192.168.116.128'
port = 2022

local_path = 'E:\\CBNB\\BackTestSystem\\strategy'
remote_path = '/shared/kungfu/strategy'
strategy = ['deviation']

file_nm = 'new_wgts_%s.csv' % datetime.now().strftime('%y%m%d')

transport = paramiko.Transport((host, port))
transport.connect(username='root', password='123456')
sftp = paramiko.SFTPClient.from_transport(transport)

for s in strategy:
    local_file = os.path.join(local_path, s, file_nm)
    remote_file = remote_path + '/' + file_nm  # os.path.join(remote_path, file_nm)
    # print remote_file
    # print pd.read_csv(local_file)
    # print remote_file

    sftp.put(local_file, remote_file)

transport.close()



