# -*- coding: utf-8 -*-
"""
Created on Thu Nov  1 10:37:41 2018

@author: jisi

首期 · 投资人评选框架

最成功投资人：收益最高
截止2018年9月30日，投资收益排名前五的用户，目前第三季度，第一名投资人收益13119朵鲜花，第五名4602花。
   （之后建议此项按季度汇总，一则可沿用目前已有的结算体系，二则投资收益线较长，按月统计时间略短）
"""

import pandas as pd
from cg_jisi.utils_time import now_date_str
from cg_jisi.utils_mail import mail_send
from cg_jisi.utils_sql import UgcUserOn3402, UgcUserOn3406, df_from_sql


def get_data(season):
    sql = f"""
    SELECT
    org_user_game_invest_dividend_award.uid,
    org_user_game_invest_dividend_award.gindex,
    org_user_game_invest_dividend_award.flower_num,
    org_user_game_invest_dividend_award.season
    FROM
    dbbh_website.org_user_game_invest_dividend_award
"""
#    WHERE
#    season = {season}
    res = df_from_sql(UgcUserOn3406, sql, ['uid', 'gindex', 'flower_num', 'season'])
    return res


def get_nickname(tup):
    df = []
    for i in range(100):
        print(i)
        res = df_from_sql(UgcUserOn3402, f"SELECT uid,nick_name FROM dbbh_passport.uc_user_info_{i} WHERE uid in {tup}", ['uid', 'name'])
        df.append(res)
    df = pd.concat(df, ignore_index=True)
    return df


if __name__ == '__main__':
    date = now_date_str('%Y%m')
    quarter = date[:4] + str((int(date[4:6]) - 1) // 3 + 1)

    s1 = get_data(quarter)
    f1 = s1.groupby(['season', 'uid'], as_index=False).agg({'flower_num': 'sum'})
    f1.sort_values(by='flower_num', ascending=False, inplace=True)
    f1 = f1.groupby('season').head(500).sort_values(by=['season', 'flower_num'], ascending=False)
    df = get_nickname(tuple(set(f1['uid'])))
    f1 = pd.merge(f1, df, how='left', on='uid')
    f1['year'] = [i // 10 for i in f1['season']]
    f1['ji'] = [i % 10 for i in f1['season']]
    f1 = f1[['year', 'ji', 'uid', 'name', 'flower_num']]
    f1.columns = ['年份', '季度', '用户编号', '用户名', '花篮花+ios花']

    f1.to_excel(f'P:/b/{quarter}最成功投资人.xlsx', sheet_name='最成功投资人前1000', index=False)
    mail_send('季度最成功投资人', ['shanchui@66rpg.com'], _attachment_path=f'P:/b/{quarter}最成功投资人.xlsx', _attachment_name='table')
