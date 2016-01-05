#conding=utf8
import pandas as pd
import cx_Oracle
import os
import pymongo
from pymongo import MongoClient
import json
from datetime import datetime
import yaml
from pandas import DataFrame, Series

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

# 设定本地数据库
db = MongoClient('localhost', 27017)['devdb']

#设定Oracle字符集，服务器字符集为AMERICAN_AMERICA.ZHS16GBK
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.ZHS16GBK'

# 设置连接tns
tns_name_135 = cx_Oracle.makedsn('132.122.151.135','1521','minos')
tns_name_94 = cx_Oracle.makedsn('132.122.155.94','1521','minos')

connstr_list = [['WXIT','Abcd1234',tns_name_135], ['WXIT','password',tns_name_94]]
for connstr in connstr_list:
    conn = cx_Oracle.connect(connstr[0], connstr[1], connstr[2])
    cur = conn.cursor()
    # 查询语句，查询属于OMMB和EMS_RM4X的所有用户表
    sql="select owner,table_name from all_tables where owner='OMMB' or owner='EMS_RM4X'"

    # tb_list为查询出的['owner','table_name']的集合
    tb_list = cur.execute(sql)
    tb_list = list(tb_list)[:1]
    for tb in tb_list:
        sql = 'select * from %s.%s t' %(tb[0], tb[1])
        df = pd.read_sql(sql, conn)
        if not df.empty:
            print('start')
            db[tb[1]].insert(json.loads(df.T.to_json()).values())
        else:
            print('fail')
    # for row in list(tb)[5]:
    #     print(row)
    # print(type(tb))
    # df_tn = pd.read_sql(sql, conn)

    # print(df_tn.head())

# 一、第一个服务器：132.122.151.135
# conn =cx_Oracle.connect('WXIT','Abcd1234',tns_name)

# 1, 查询所有表的记录数
# sql='select owner, table_name, num_rows from all_tables'

# 2，查询FDD_EUTRANCELLFDD_P_V2的记录数
# sql='select * from OMMB.FDD_EUTRANCELLFDD_P_V2 t'
# df=pd.read_sql(sql, conn)
# df.to_csv('FDD_EUTRANCELLFDD_P_V2.csv')
