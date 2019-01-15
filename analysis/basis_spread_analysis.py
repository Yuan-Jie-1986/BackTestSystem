# coding=utf-8

from lib.simulator.base import BacktestSys
import re
import matplotlib.pyplot as plt
import numpy as np
import pprint
import pandas as pd
import talib

class BasisSpreadAnalysis(BacktestSys):
    def __init__(self):
        super(BasisSpreadAnalysis, self).__init__()

    def analysis(self):
        cmd_dict = {}
        for k in self.category:
            cmd_dict.setdefault(self.category[k], []).append(k)

        stats_df = pd.DataFrame()
        for cmd in cmd_dict.keys():
            # cmd = 'TA'
            ptn = re.compile('_MC_LastMonthEnd')
            for nm in cmd_dict[cmd]:
                if ptn.search(nm):
                    futures = self.data[nm]['CLOSE']
                    remains = self.data[nm]['remain_days']
                else:
                    spot = self.data[nm].values()[0]
            spot = pd.DataFrame(spot).fillna(method='ffill').values.flatten()
            # slope_period = 20
            # spot_slope = talib.LINEARREG_SLOPE(spot, timeperiod=slope_period)
            # fut_slope = talib.LINEARREG_SLOPE(futures, timeperiod=slope_period)
            # slope_diff = spot_slope - fut_slope

            basis = spot - futures
            # ## 分析剩余天数对收益的影响情况
            # times = 0
            # pre_days = 40
            # res_dict = {}
            # open_flag = False
            #
            # for i in np.arange(len(self.dt)):
            #
            #     if np.isnan(basis[i]) or np.isnan(spot[i]) or np.isnan(futures[i]):
            #         continue
            #
            #     # if i == 0 or remains[i] > remains[i-1]:
            #     if remains[i] == pre_days or (remains[i-1] > pre_days > remains[i]):
            #         times += 1
            #         res_dict[times] = {'basis_open': basis[i],
            #                            'fut_open': futures[i],
            #                            'dt_open': self.dt[i],
            #                            'remain_open': remains[i],
            #                            'direction': 1 if basis[i] > 0 else -1,
            #                            'spot_open': spot[i],
            #                            'spot_slope': spot_slope[i],
            #                            'fut_slope': fut_slope[i],
            #                            'slope_diff': slope_diff[i],
            #                            'cmd': cmd,
            #                            'count': times}
            #         open_flag = True
            #         # plt.plot(spot[i-slope_period+1: i+1], color='r')
            #         # plt.plot(futures[i-slope_period+1: i+1], color='k')
            #
            #     elif open_flag and (i == len(self.dt) - 1 or remains[i] < remains[i + 1]):
            #         if times not in res_dict or 'dt_close' in res_dict[times]:
            #             print times, self.dt[i], res_dict[times]
            #             raise Exception(u'没有之前的开仓数据')
            #         res_dict[times]['basis_close'] = basis[i]
            #         res_dict[times]['fut_close'] = futures[i]
            #         res_dict[times]['spot_close'] = spot[i]
            #         res_dict[times]['dt_close'] = self.dt[i]
            #         res_dict[times]['remain_close'] = remains[i]
            #         res_dict[times]['basis_rtn'] = (res_dict[times]['basis_close'] - res_dict[times]['basis_open']) * \
            #                                        (-res_dict[times]['direction'])
            #         res_dict[times]['fut_rtn'] = (res_dict[times]['fut_close'] - res_dict[times]['fut_open']) * \
            #                                        res_dict[times]['direction']
            #         open_flag = False
            #
            #         # pprint.pprint(res_dict[times])
            #         # plt.show()
            #
            # basis_rtn = []
            # fut_rtn = []
            # times = []
            #
            # for d in res_dict:
            #     times.append(d)
            #     basis_rtn.append(res_dict[d]['basis_rtn'])
            #     fut_rtn.append(res_dict[d]['fut_rtn'])
            # times = np.array(times)
            # basis_rtn = np.array(basis_rtn)
            # fut_rtn = np.array(fut_rtn)
            #
            # total = len(times)
            # BasisCorr = len(basis_rtn[basis_rtn > 0.])
            # BasisPct = BasisCorr * 1. / len(basis_rtn)
            # BasisRtn = np.mean(basis_rtn)
            # FutCorr = len(fut_rtn[fut_rtn > 0.])
            # FutPct = FutCorr * 1. / len(fut_rtn)
            # FutRtn = np.mean(fut_rtn)


            # print u'==========%s的统计结果===========' % cmd
            # print u'总次数：', total
            # print u'Basis准确率：', BasisPct
            # print u'Basis准确次数：', BasisCorr
            # print u'Basis平均盈利：', BasisRtn
            # print u'Futures准确率：', FutPct
            # print u'Futures准确次数：', FutCorr
            # print u'Futures平均盈利：', FutRtn
            #
            # buy_rtn = np.array([res_dict[d]['fut_rtn'] for d in res_dict if res_dict[d]['direction'] == 1])
            # if len(buy_rtn) > 0:
            #     buy_counter = len(buy_rtn)
            #     buy_correct = len(buy_rtn[buy_rtn > 0])
            #     buy_correct_pct = buy_correct * 1. / buy_counter
            #     buy_rtn_avg = np.mean(buy_rtn)
            #     print u'Futures做多次数：', buy_counter
            #     print u'Futures做多做对次数：', buy_correct
            #     print u'Futures做多准确率：', buy_correct_pct
            #     print u'Futures平均做多收益：', buy_rtn_avg
            #
            # sell_rtn = np.array([res_dict[d]['fut_rtn'] for d in res_dict if res_dict[d]['direction'] == -1])
            # if len(sell_rtn) > 0:
            #     sell_counter = len(sell_rtn)
            #     sell_correct = len(sell_rtn[sell_rtn > 0])
            #     sell_correct_pct = sell_correct * 1. / sell_counter
            #     sell_rtn_avg = np.mean(sell_rtn)
            #
            #     print u'Futures做空次数：', sell_counter
            #     print u'Futures做空做对次数：', sell_correct
            #     print u'Futures做空准确率：', sell_correct_pct
            #     print u'Futures平均做空收益：', sell_rtn_avg
            #
            #
            #
            # stats_df = pd.concat([stats_df, pd.DataFrame.from_dict(res_dict, orient='index')])

            # width = 0.4
            # plt.bar(times, basis_rtn, width, color='r', label='BasisRtn')
            # plt.bar(times+width, fut_rtn, width, color='k', label='FutRtn')
            # plt.legend()
            # plt.grid()
            # plt.title('%s yield' % cmd)
            # plt.show()
            # pprint.pprint(res_dict)

        # rtn_total = stats_df['fut_rtn'].values.flatten()
        # rtn_counter = len(rtn_total)
        # rtn_pos = len(rtn_total[rtn_total > 0])
        # rtn_neg = len(rtn_total[rtn_total < 0])
        # print u'======================='
        # print u'总数：', rtn_counter
        # print u'正收益个数：', rtn_pos
        # print u'负收益个数：', rtn_neg
        # rtn_buy_total = stats_df[stats_df['direction'] == 1]['fut_rtn'].values.flatten()
        # rtn_buy_counter = len(rtn_buy_total)
        # rtn_buy_pos = len(rtn_buy_total[rtn_buy_total > 0])
        # rtn_buy_pos_pct = rtn_buy_pos * 1. / rtn_buy_counter
        # rtn_buy_avg = np.mean(rtn_buy_total)
        # print u'======================='
        # print u'做多次数：', rtn_buy_counter
        # print u'做多正确次数：', rtn_buy_pos
        # print u'做多正确率：', rtn_buy_pos_pct
        # print u'做多平均收益：', rtn_buy_avg
        # rtn_sell_total = stats_df[stats_df['direction'] == -1]['fut_rtn'].values.flatten()
        # rtn_sell_counter = len(rtn_sell_total)
        # rtn_sell_pos = len(rtn_sell_total[rtn_sell_total > 0])
        # rtn_sell_pos_pct = rtn_sell_pos * 1. / rtn_sell_counter
        # rtn_sell_avg = np.mean(rtn_sell_total)
        # print u'======================='
        # print u'做空次数：', rtn_sell_counter
        # print u'做空正确次数：', rtn_sell_pos
        # print u'做空正确率：', rtn_sell_pos_pct
        # print u'做空平均收益：', rtn_sell_avg
        #
        # ## 统计基差与现货趋势相同的时候交易结果
        # rtn_bs_trend_total = stats_df[stats_df['direction'] * stats_df['spot_slope'] > 0.]['fut_rtn'].values.flatten()
        # rtn_bs_trend_count = len(rtn_bs_trend_total)
        # rtn_bs_trend_pos = len(rtn_bs_trend_total[rtn_bs_trend_total > 0.])
        # rtn_bs_trend_pos_pct = rtn_bs_trend_pos * 1. / rtn_bs_trend_count
        # rtn_bs_trend_avg = np.mean(rtn_bs_trend_total)
        # print u'======================='
        # print u'基差与趋势相同的交易次数：', rtn_bs_trend_count
        # print u'基差与趋势相同的正确交易次数：', rtn_bs_trend_pos
        # print u'基差与趋势相同的交易正确率：', rtn_bs_trend_pos_pct
        # print u'基差与趋势相同的交易平均收益：', rtn_bs_trend_avg
        #
        # ## 统计基差与现货趋势相反的时候交易结果
        # rtn_bs_trend_total = stats_df[stats_df['direction'] * stats_df['spot_slope'] < 0.]['fut_rtn'].values.flatten()
        # rtn_bs_trend_count = len(rtn_bs_trend_total)
        # rtn_bs_trend_pos = len(rtn_bs_trend_total[rtn_bs_trend_total > 0.])
        # rtn_bs_trend_pos_pct = rtn_bs_trend_pos * 1. / rtn_bs_trend_count
        # rtn_bs_trend_avg = np.mean(rtn_bs_trend_total)
        # print u'======================='
        # print u'基差与趋势相反的交易次数：', rtn_bs_trend_count
        # print u'基差与趋势相反的正确交易次数：', rtn_bs_trend_pos
        # print u'基差与趋势相反的交易正确率：', rtn_bs_trend_pos_pct
        # print u'基差与趋势相反的交易平均收益：', rtn_bs_trend_avg
        #
        # ## 考虑基差与趋势差相同的情况
        # rtn_bs_trendiff_total = stats_df[stats_df['direction'] * stats_df['slope_diff'] > 0.]['fut_rtn'].values.flatten()
        # rtn_bs_trendiff_count = len(rtn_bs_trendiff_total)
        # rtn_bs_trendiff_pos = len(rtn_bs_trendiff_total[rtn_bs_trendiff_total > 0.])
        # rtn_bs_trendiff_pos_pct = rtn_bs_trendiff_pos * 1. / rtn_bs_trendiff_count
        # rtn_bs_trendiff_avg = np.mean(rtn_bs_trendiff_total)
        # print u'======================='
        # print u'基差与趋势差相同的交易次数：', rtn_bs_trendiff_count
        # print u'基差与趋势差相同的正确交易次数：', rtn_bs_trendiff_pos
        # print u'基差与趋势差相同的交易正确率：', rtn_bs_trendiff_pos_pct
        # print u'基差与趋势差相同的交易平均收益：', rtn_bs_trendiff_avg
        #
        # ## 考虑基差与趋势差相反的情况
        # rtn_bs_trendiff_total = stats_df[stats_df['direction'] * stats_df['slope_diff'] < 0.][
        #     'fut_rtn'].values.flatten()
        # rtn_bs_trendiff_count = len(rtn_bs_trendiff_total)
        # rtn_bs_trendiff_pos = len(rtn_bs_trendiff_total[rtn_bs_trendiff_total > 0.])
        # rtn_bs_trendiff_pos_pct = rtn_bs_trendiff_pos * 1. / rtn_bs_trendiff_count
        # rtn_bs_trendiff_avg = np.mean(rtn_bs_trendiff_total)
        # print u'======================='
        # print u'基差与趋势差相反的交易次数：', rtn_bs_trendiff_count
        # print u'基差与趋势差相反的正确交易次数：', rtn_bs_trendiff_pos
        # print u'基差与趋势差相反的交易正确率：', rtn_bs_trendiff_pos_pct
        # print u'基差与趋势差相反的交易平均收益：', rtn_bs_trendiff_avg
        #
        # ## 考虑基差与趋势相同、与趋势差相反的情况
        # rtn_bs_trend_trendiff_total = stats_df[(stats_df['direction'] * stats_df['slope_diff'] < 0.) &
        #                                        (stats_df['direction'] * stats_df['spot_slope'] > 0.)][
        #     'fut_rtn'].values.flatten()
        # rtn_bs_trend_trendiff_count = len(rtn_bs_trend_trendiff_total)
        # rtn_bs_trend_trendiff_pos = len(rtn_bs_trend_trendiff_total[rtn_bs_trend_trendiff_total > 0.])
        # rtn_bs_trend_trendiff_pos_pct = rtn_bs_trend_trendiff_pos * 1. / rtn_bs_trend_trendiff_count
        # rtn_bs_trend_trendiff_avg = np.mean(rtn_bs_trend_trendiff_total)
        # print u'======================='
        # print u'基差与趋势差相反、趋势相同的交易次数：', rtn_bs_trend_trendiff_count
        # print u'基差与趋势差相反、趋势相同的正确交易次数：', rtn_bs_trend_trendiff_pos
        # print u'基差与趋势差相反、趋势相同的交易正确率：', rtn_bs_trend_trendiff_pos_pct
        # print u'基差与趋势差相反、趋势相同的交易平均收益：', rtn_bs_trend_trendiff_avg
        #
        # ## 考虑基差与趋势相同、与趋势差相同的情况
        # rtn_bs_trend_trendiff_total = stats_df[(stats_df['direction'] * stats_df['slope_diff'] > 0.) &
        #                                        (stats_df['direction'] * stats_df['spot_slope'] > 0.)][
        #     'fut_rtn'].values.flatten()
        # rtn_bs_trend_trendiff_count = len(rtn_bs_trend_trendiff_total)
        # rtn_bs_trend_trendiff_pos = len(rtn_bs_trend_trendiff_total[rtn_bs_trend_trendiff_total > 0.])
        # rtn_bs_trend_trendiff_pos_pct = rtn_bs_trend_trendiff_pos * 1. / rtn_bs_trend_trendiff_count
        # rtn_bs_trend_trendiff_avg = np.mean(rtn_bs_trend_trendiff_total)
        # print u'======================='
        # print u'基差与趋势差相同、趋势相同的交易次数：', rtn_bs_trend_trendiff_count
        # print u'基差与趋势差相同、趋势相同的正确交易次数：', rtn_bs_trend_trendiff_pos
        # print u'基差与趋势差相同、趋势相同的交易正确率：', rtn_bs_trend_trendiff_pos_pct
        # print u'基差与趋势差相同、趋势相同的交易平均收益：', rtn_bs_trend_trendiff_avg
        #
        # ## 考虑基差与趋势相反、与趋势差相同的情况
        # rtn_bs_trend_trendiff_total = stats_df[(stats_df['direction'] * stats_df['slope_diff'] > 0.) &
        #                                        (stats_df['direction'] * stats_df['spot_slope'] < 0.)][
        #     'fut_rtn'].values.flatten()
        # rtn_bs_trend_trendiff_count = len(rtn_bs_trend_trendiff_total)
        # rtn_bs_trend_trendiff_pos = len(rtn_bs_trend_trendiff_total[rtn_bs_trend_trendiff_total > 0.])
        # rtn_bs_trend_trendiff_pos_pct = rtn_bs_trend_trendiff_pos * 1. / rtn_bs_trend_trendiff_count
        # rtn_bs_trend_trendiff_avg = np.mean(rtn_bs_trend_trendiff_total)
        # print u'======================='
        # print u'基差与趋势差相同、趋势相反的交易次数：', rtn_bs_trend_trendiff_count
        # print u'基差与趋势差相同、趋势相反的正确交易次数：', rtn_bs_trend_trendiff_pos
        # print u'基差与趋势差相同、趋势相反的交易正确率：', rtn_bs_trend_trendiff_pos_pct
        # print u'基差与趋势差相同、趋势相反的交易平均收益：', rtn_bs_trend_trendiff_avg
        #
        # ## 考虑基差与趋势相反、与趋势差相反的情况
        # rtn_bs_trend_trendiff_total = stats_df[(stats_df['direction'] * stats_df['slope_diff'] < 0.) &
        #                                        (stats_df['direction'] * stats_df['spot_slope'] < 0.)][
        #     'fut_rtn'].values.flatten()
        # rtn_bs_trend_trendiff_count = len(rtn_bs_trend_trendiff_total)
        # rtn_bs_trend_trendiff_pos = len(rtn_bs_trend_trendiff_total[rtn_bs_trend_trendiff_total > 0.])
        # rtn_bs_trend_trendiff_pos_pct = rtn_bs_trend_trendiff_pos * 1. / rtn_bs_trend_trendiff_count
        # rtn_bs_trend_trendiff_avg = np.mean(rtn_bs_trend_trendiff_total)
        # print u'======================='
        # print u'基差与趋势差相反、趋势相反的交易次数：', rtn_bs_trend_trendiff_count
        # print u'基差与趋势差相反、趋势相反的正确交易次数：', rtn_bs_trend_trendiff_pos
        # print u'基差与趋势差相反、趋势相反的交易正确率：', rtn_bs_trend_trendiff_pos_pct
        # print u'基差与趋势差相反、趋势相反的交易平均收益：', rtn_bs_trend_trendiff_avg

        # print stats_df
        # plt.hist(stats_df['fut_rtn'].values, bins=100)

        # plt.show()
        # stats_df.to_csv('basis_spread_40days.csv')
        ## 绘图
            plt.subplot(311)
            plt.title('The Basis Spread Analysis of %s' % cmd)
            plt.plot_date(self.dt, futures, fmt='-r', label='Futures')
            plt.plot_date(self.dt, spot, fmt='-k', label='Spot')
            x_axis = plt.gca().xaxis
            plt.grid()
            plt.legend(loc='best')
            plt.subplot(312)
            plt.plot_date(self.dt, basis, fmt='-g', label='BasisSpread')
            plt.xlim(x_axis)
            plt.grid()
            plt.legend(loc='best')
            plt.subplot(313)
            plt.plot_date(self.dt, remains, fmt='-r', label='Remain Days')
            plt.xlim(x_axis)
            plt.grid()
            plt.legend(loc='best')

            plt.show()


if __name__ == '__main__':
    a = BasisSpreadAnalysis()
    a.analysis()