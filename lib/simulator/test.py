# coding=utf-8
import yaml
import os
import pymongo
from datetime import datetime
import pandas as pd
import numpy as np
import pprint
from base import BacktestSys


class test(BacktestSys):
    def __init__(self):
        self.current_file = __file__
        self.prepare()


if __name__ == '__main__':

    a = test()
    a.getPnl({'PP.DCE': np.random.randint(-1, 2, 1438),
              'TA.CZC': np.random.randint(-1, 2, 1438)})