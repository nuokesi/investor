"""
Created on Mon Oct 30 18:15:07 2018

@author: jisi

首期 · 投资人评选框架

2、最佳星探：当月首个发掘出好作品的用户
在10月编推（即成功结算）的30款作品中，每个作品首位投资用户，前十名
规则：按鲜花数排名，如有重复，按照投资时间排序，人选重复，则顺延。

3、资产丰富：投资项目最多
10月内新投资作品个数最多的用户，前三名

4、独具慧眼：投资了关注度低的作品且成功的投资人
在当月编推完成结算后，投资人数最少的五款作品中，投资金额排名前五的用户

5、投资新手：首次投资成功的用户
以当月结算为时间节点，首次拿到投资回报的用户，且投资金额排名前五名

6、大家都看好的作品：当月最受欢迎的投资作品
投资人数最多的五款作品，按照三个投资期划分
（新增个数如果相对较少，则按照投资金额统计）
"""

import datetime
import pandas as pd
from cg_jisi.utils_mail import mail_send
from cg_jisi.utils_time import before_month
from cg_jisi.utils_sql import UgcUserOn3402, UgcUserOn3406, df_from_sql, get_res


def get_data(e):
    sql = f"""
    SELECT
    uid,
    gindex,
    flower_num,
    invest_status,
    create_time
    FROM
    dbbh_website.org_user_game_invest
    WHERE
    invest_status != 2 AND
    create_time < '{e}'
"""
    res = df_from_sql(UgcUserOn3406, sql, ['uid', 'gindex', 'flower_num', 'status', 'create_time'])
    return res


def get_gindex_level(tup):
    sql = f"""
    SELECT
    gindex,
    `level`
    FROM
    dbbh_website.org_auto_promo_game
    WHERE
    gindex in {tup}
    """
    res = df_from_sql(UgcUserOn3406, sql, ['gindex', 'level'])
    return res


def get_nickname(tup):
    dataframe = []
    for idx in range(100):
        print(idx)
        res = df_from_sql(UgcUserOn3402,
                          f"SELECT uid,nick_name FROM dbbh_passport.uc_user_info_{idx} WHERE uid in {tup}", ['uid', 'name'])
        dataframe.append(res)
    dataframe = pd.concat(dataframe, ignore_index=True)
    return dataframe


def get_biantui(s, e):
    sql = f"""
    SELECT distinct 
    "作品编号"
    FROM 
    daily_game_static_info
    WHERE 
    "编推" >= '{s}' AND
    "编推"<'{e}' AND
    "作品编号" not in (select distinct "作品编号"
                        FROM daily_game_static_info
                        WHERE "编推" < '{s}');
    """
    res = get_res('warehouse', sql)
    return tuple([r[0] for r in res])


def get_gain(tup):
    sql = f"""
    SELECT
    uid,
    gindex,
    flower_num,
    voucher_1_num,
    voucher_2_num,
    voucher_3_num,
    voucher_4_num,
    create_time    
    FROM
    dbbh_website.org_user_game_invest_dividend_award
    WHERE
    gindex in {tup} and
    id in (SELECT min(id)
            from dbbh_website.org_user_game_invest_dividend_award
            WHERE flower_num != 0
            GROUP BY uid)
    """
    res = df_from_sql(UgcUserOn3406, sql, ['uid', 'gindex', 'flower', 'v1', 'v2', 'v3', 'v4', 'create_time'])
    return res


if __name__ == '__main__':

    i = -1
    start = before_month(i)[:4] + '-' + before_month(i)[4:6] + '-01'
    end = before_month((1 + i))[:4] + '-' + before_month((1 + i))[4:6] + '-01'
    
    s1 = get_data(end)
    s2 = get_nickname(tuple(set(s1['uid'])))
    s3 = get_biantui(start, end)  # 编推作品元组
    s4 = s1[[i in s3 for i in s1['gindex']]]  # 编推作品记录
    s5 = s1[s1['create_time'] < datetime.datetime(int(start[:4]), int(start[5:7]), 1)]  # 计算月份 之前所有记录

    f1 = s4.groupby('gindex', as_index=False).head(1)                  # f1编推作品 首次投资记录
    df = s4[[i in list(set(f1['uid'])) for i in s4['uid']]]            # 编推作品中 首次投资用户的记录
    df = df.groupby('uid', as_index=False).agg({'flower_num': 'sum'})  # 编推作品中 首次投资用户的投资总花数
    f1 = pd.merge(f1, df, how='left', on='uid', sort=False)
    f1 = pd.merge(f1, s2, how='left', on='uid', sort=False)
    f1.sort_values(by='flower_num_y', ascending=False, inplace=True)
    f1 = f1[['gindex', 'uid', 'name', 'flower_num_y', 'flower_num_x', 'create_time']]
    f1.columns = ['作品ID', '首位投资用户ID', '用户昵称', '本月编推作品投资花数', '首次投资的花数', '投资时间']

    df = s5.groupby(['uid', 'gindex'], as_index=False).agg({'flower_num': 'count'})
    df = df.groupby('uid', as_index=False).agg({'flower_num': 'count'})              # 月初用户投资作品数
    f2 = s1.groupby(['uid', 'gindex'], as_index=False).agg({'flower_num': 'count'})
    f2 = f2.groupby('uid', as_index=False).agg({'flower_num': 'count'})              # 月末用户投资作品数
    f2 = pd.merge(f2, df, how='left', on='uid', sort=False)
    f2 = pd.merge(f2, s2, how='left', on='uid', sort=False)
    f2 = f2.fillna(0)
    f2['flower_num'] = f2['flower_num_x'] - f2['flower_num_y']
    f2.sort_values(by='flower_num', ascending=False, inplace=True)
    f2 = f2[['uid', 'name', 'flower_num_x', 'flower_num_y', 'flower_num']]
    f2.columns = ['用户ID', '用户昵称', '月初投资作品数', '月末投资作品数', '新增投资作品数']

    f3 = s4.groupby(['gindex', 'uid'], as_index=False).sum()           # 编推作品（作品用户）集合
    f3 = f3.groupby('gindex', as_index=False).agg({'uid': 'count'})    # 为编推作品投资用户数
    f3.sort_values(by='uid', inplace=True)
    f7 = f3[['gindex', 'uid']]
    f7.columns = ['作品ID', '投资人数']
    f3 = s1[[i in tuple(f3[:5]['gindex']) for i in s1['gindex']]]      # 关注度最低的五个作品的所有记录
    f3 = f3.groupby('uid', as_index=False).agg({'flower_num': 'sum'})  # 用户为5个作品的投资金额
    f3 = pd.merge(f3, s2, how='left', on='uid', sort=False)
    f3.sort_values(by='flower_num', ascending=False, inplace=True)
    f3 = f3[['uid', 'name', 'flower_num']]
    f3.columns = ['用户ID', '用户昵称', '关注度最低5个作品投资总花数']

    f4 = get_gain(s3)                                                  # 用户首次分红记录作品编号为本次编推作品的记录
    df = s1[[i in tuple(set(f4['uid'])) for i in s1['uid']]]           # 首次分成用户的所有记录
    df = df.groupby('uid', as_index=False).agg({'flower_num': 'sum'})  # 首次分成用户投资所有游戏的花数
    f4 = pd.merge(f4, df, how='left', on='uid', sort=False)
    f4 = pd.merge(f4, s2, how='left', on='uid', sort=False)
    f4.sort_values(by='flower_num', ascending=False, inplace=True)
    f4 = f4[['uid', 'name', 'flower_num', 'gindex', 'flower', 'v1', 'v2', 'v3', 'v4', 'create_time']]
    f4.columns = ['首次投资成功用户ID', '首次投资成功用户昵称', '投资所有游戏的总金额', '第一次分红作品', '花数', '充一赠一劵', '充一赠二劵', '充一赠三劵', '充一赠四劵', '第一次结算时间']

    df = s5.groupby(['gindex', 'uid'], as_index=False).agg({'flower_num': 'sum'})         # 月初用户为作品投资总额
    df = df.groupby('gindex', as_index=False).agg({'uid': 'count', 'flower_num': 'sum'})  # 月初为作品投资的用户数量  月初作品被投资的金额
    f5 = s1.groupby(['gindex', 'uid'], as_index=False).agg({'flower_num': 'sum'})         # 月末用户为作品投资总额
    f5 = f5.groupby('gindex', as_index=False).agg({'uid': 'count', 'flower_num': 'sum'})  # 月末为作品投资的用户数量  月末作品被投资的金额
    f5 = pd.merge(f5, df, how='left', on='gindex')
    f5 = f5.fillna(0)
    f5['uid'] = f5['uid_x'] - f5['uid_y']
    f5['flower_num'] = f5['flower_num_x'] - f5['flower_num_y']
    df = get_gindex_level(tuple(set(f5['gindex']))).fillna(1)
    f5 = pd.merge(f5, df, how='left', on='gindex', sort=False)
    f5.sort_values(by=['level', 'uid'], ascending=False, inplace=True)
    f5 = f5[['level', 'gindex', 'uid_x', 'uid_y', 'uid', 'flower_num_x', 'flower_num_y', 'flower_num']]
    f5.columns = ['作品当前等级', '作品ID', '月末投资总人数', '月初投资总人数', '投资新增人数', '月末投资花数', '月初投资花数', '投资新增总花数']

    writer = pd.ExcelWriter(f"P:/b/{before_month(i)}.xlsx")
    f1.to_excel(writer, sheet_name='最佳星探', index=False)
    f2.to_excel(writer, sheet_name='资产丰富', index=False)
    f3.to_excel(writer, sheet_name='独具慧眼', index=False)
    f4.to_excel(writer, sheet_name='投资新手', index=False)
    f5.to_excel(writer, sheet_name='大家都看好的作品', index=False)
    f7.to_excel(writer, sheet_name='作品关注度', index=False)
    f8 = pd.DataFrame(list(s3), columns=['作品ID'])
    f8.to_excel(writer, sheet_name='编推作品', index=False)
    writer.save()

    mail_send('每月_投资人评选', ['shanchui@66rpg.com'], _attachment_path=f"P:/b/{before_month(i)}.xlsx", _attachment_name=before_month(i))
