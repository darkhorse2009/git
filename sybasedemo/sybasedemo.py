#coding=utf8
import pandas as pd
from pandas import DataFrame, Series
import pyodbc
import pymongo
from pymongo import MongoClient
import json
from datetime import datetime
import yaml

# 如果字符串为数字，则转化
def isnumeric(s):
    if all(c in "0123456789.+-" for c in s) and any(c in "0123456789" for c in s):
        return yaml.load(s)
    else:
        return s

#调整dataframe显示顺序函数
def set_column_sequence(dataframe, seq):
    '''Takes a dataframe and a subsequence of its columns, returns dataframe with seq as first columns'''
    cols = seq[:] # copy so we don't mutate seq
    for x in dataframe.columns:
        if x not in cols:
            cols.append(x)
    return dataframe[cols]

# 服务器列表
DSN_LIST = [['ODBC69','wxzx','Wxzx1234']]
# 连接数据库
db = MongoClient('localhost', 27017)['devdb']
# 当前日期
current_date = datetime.now().strftime('%Y%m%d')

for DSN in DSN_LIST:
    print(DSN[0])
    # connstr= 'DSN=%s;UID=wxzx;PWD=Wxzx1234' % DSN
    connstr= 'DSN=%s;UID=%s;PWD=%s' % (DSN[0],DSN[1],DSN[2])
    # print(connstr)
    # print(connstr)
    conn = pyodbc.connect(connstr)
    # conn = pyodbc.connect('DSN=ODBC69;UID=wxzx;PWD=Wxzx1234')
    cur = conn.cursor()
    # sql_tb = "select name from sysobjects where type = 'U'"
    sql_tb = "select name from sysobjects where type = 'U' and name like 't_C_%'"
    tbnames = cur.execute(sql_tb) # 取出数据库所有表名
    tb_list = []
    # 转化为list，如果直接for循环，下步的cur.execute将冲掉，导致tbname发生变化
    for tbname_t in tbnames:
        tb_list.append(tbname_t[0])
    # 以ENODEBFUNCTION作为基准表，按ENODEBID筛选出佛山，佛山ENODEBID范围为7A000~7A7FF、86800~86FFF，其他表按照筛选表的PHYID筛选

    df_right_dict = {}
    for name in ['LA0O', 'LCAG', 'LK0Q']:
        tbname = 't_C_ENODEBFUNCTION' + '_' + name
        sql_enodeb = "select * from %s" %tbname
        df_right = pd.read_sql(sql_enodeb, conn)
        # df['CreateDate'] = datetime.now().strftime('%Y%m%d')
        df_right_dict[tbname] = df_right[((df_right['ENODEBID'] >= 499712) & (df_right['ENODEBID'] <= 503807)) | ((df_right['ENODEBID'] >= 550912) & (df_right['ENODEBID'] <= 552959))]
        # print(df_right_dict[tbname])
    # 循环查出表的所有字段
    # tb_list = ['t_C_ENODEBFUNCTION_LA0O','t_C_ENODEBFUNCTION_LCAG','t_C_ENODEBFUNCTION_LK0Q','t_C_CELLALGOSWITCH_LA0O','t_C_CELLALGOSWITCH_LCAG','t_C_CELLALGOSWITCH_LK0Q']
    for tbname in tb_list:
        if tbname.split('_')[-1] in ['LA0O', 'LCAG', 'LK0Q']:
            tbname_right = '_'.join(['t_C_ENODEBFUNCTION',tbname.split('_')[-1]])
            #print(tbname)
            #fobj.writelines(tbname+'\n')
            #tbname = 't_C_ENODEBFUNCTION_LK0Q'
            sql_enodeb = "select * from %s" %tbname
            df_left = pd.read_sql(sql_enodeb, conn)
            if not df_left.empty:
                df_left['CreateDate'] = datetime.now().strftime('%Y%m%d')
                colsname_left = df_left.columns
                df = pd.merge(df_left, df_right_dict[tbname_right], on=['PHYID'], suffixes=['','_r'])[colsname_left]
                if not df.empty:
                    db[tbname].insert(json.loads(df.T.to_json()).values())
                    print(tbname, 'has been inserted!')
                else:
                    print(tbname, 'is an empty dataframe!')
            else:
                print(tbname, 'is an empty dataframe!')
