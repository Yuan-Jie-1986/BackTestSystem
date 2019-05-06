# coding=utf-8
import numpy as np
import pandas as pd

class FactorAnalysis(object):
    def __init__(self, fac, cls):
        # fac和cls是dataframe的形式，index是日期
        self.factor = fac
        self.fac_cls = cls

    def normal_ic(self, ic_len=20, ir_len=60):
        rtn = self.fac_cls.pct_change(periods=ic_len, fill_method='ffill')
        rtn = rtn.shift(periods=-ic_len)
        ic = self.factor.corrwith(rtn, axis=1, drop=True)
        ic = pd.DataFrame({'NormalIC': ic})
        ic_mean = ic.rolling(window=ir_len).mean()
        ic_std = ic.rolling(window=ir_len).std()
        ic['NormalIR'] = ic_mean / ic_std
        return ic

    def rank_ic(self, ic_len=20, ir_len=60):
        rtn = self.fac_cls.pct_change(periods=ic_len, fill_method='ffill')
        rtn = rtn.shift(periods=-ic_len)
        fac_rank = self.factor.rank(axis=1, na_option='keep')
        rtn_rank = rtn.rank(axis=1, na_option='keep')
        ic = fac_rank.corrwith(rtn_rank, axis=1, drop=True)
        ic = pd.DataFrame({'RankIC': ic})
        ic_mean = ic.rolling(window=ir_len).mean()
        ic_std = ic.rolling(window=ir_len).std()
        ic['RankIR'] = ic_mean / ic_std
        return ic

    def fac_rtn(self, fac_num=3, rtn_len=20, ir_len=60):
        rtn = self.fac_cls.pct_change(periods=rtn_len, fill_method='ffill')
        rtn = rtn.shift(periods=-rtn_len)
        fac_rank = self.factor.rank(axis=1, na_option='keep')
        fac_len = len(self.factor.columns)
        rtn_high = rtn.copy()
        rtn_high[fac_rank <= fac_len - fac_num] = np.nan
        rtn_low = rtn.copy()
        rtn_low[fac_rank > fac_num] = np.nan
        res = rtn_high.mean(axis=1) - rtn_low.mean(axis=1)
        res = pd.DataFrame({'FacRtn': res})
        res_mean = res.rolling(window=ir_len).mean()
        res_std = res.rolling(window=ir_len).std()
        res['RtnIR'] = res_mean / res_std

        return res

    def effective_num(self):
        return pd.DataFrame(self.fac_cls.count(axis=1), columns=['number'])






if __name__ == '__main__':
    a = pd.DataFrame(np.random.randn(10, 5))
    b = pd.DataFrame(np.random.randn(10, 5))
    b.iloc[3,2] = np.nan
    c = FactorAnalysis(a, b)
    # c.fac_rtn(fac_num=3, rtn_len=3)
    c.effective_num()




