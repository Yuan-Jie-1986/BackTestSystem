# coding=utf-8
from lib.simulator.base import BacktestSys
import numpy as np
import pandas as pd
from datetime import datetime

class Deviation(BacktestSys):
    def __init__(self):
        super(Deviation, self).__init__()

    def strategy(self):
        wgtsDict = {}
        for k in self.data:
            cls = self.data[k]['CLOSE']
            cls_df = pd.DataFrame({'CLOSE': cls}, index=self.dt)
            cls_df['mon_day'] = [d.strftime('%m-%d') for d in self.dt]
            cls_df['year'] = [d.strftime('%Y') for d in self.dt]
            season_df = cls_df.pivot(index='mon_day', columns='year', values='CLOSE')
            season_df.fillna(method='ffill', inplace=True)
            season_df = season_df.rolling(window=5, min_periods=3, axis=1).mean()
            yr = max(int(y) for y in season_df.columns) + 1
            season_df[str(yr)] = np.nan
            season_df = season_df.shift(axis=1)
            future_df = pd.DataFrame()
            for c in season_df:
                temp_idx = []
                temp_value = season_df[c]
                for d in season_df.index:
                    try:
                        temp_idx.append(datetime.strptime('%s-%s' % (c, d), '%Y-%m-%d'))
                    except ValueError:
                        temp_value.drop(index=d, inplace=True)

                temp = pd.DataFrame({'CLOSE': temp_value.values}, index=temp_idx)
                future_df = pd.concat([future_df, temp], axis=0)
            future_df = future_df.shift(periods=-10, axis=0)

            print future_df


if __name__ == '__main__':
    a = Deviation()
    a.strategy()
