# coding=utf-8

from facbank import FacGenerator as fg
from factool import FactorAnalysis as fa
import matplotlib.pyplot as plt

fgObj = fg()
fac1 = fgObj.ts_rank(fgObj.ts_sum(fgObj.rtn, period=20, min_periods=14), period=250, min_periods=200)
faObj = fa(fac1, fgObj.cls)
rank_ic = faObj.rank_ic()
rank_ic.plot(grid=True)
print rank_ic.describe()
plt.show()
