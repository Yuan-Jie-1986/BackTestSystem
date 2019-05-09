# coding=utf-8
from facbank import FacGenerator as fg
from factool import FactorAnalysis as fa

fgObj = fg()
fac = fgObj.rank(fgObj.rtn)
faObj = fa(fac, fgObj.cls)
faObj.fac_rtn(3, 1, 60)