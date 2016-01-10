from lxml import etree
import pandas as pd
from pandas import DataFrame, Series
from pymongo import MongoClient
import json
import yaml
import io
import re
import zipfile
import rarfile
import gzip
import os
from datetime import datetime

# 当前日期
current_date = datetime.now().strftime('%Y%m%d')
# 连接数据库
db = MongoClient('localhost', 27017)['test']
# 压缩文件格式
extension = ['.rar', '.zip', '.gz']


def isnumeric(s):
    if all(c in "0123456789.+-" for c in s) and any(c in "0123456789" for c in s):
        if s.isdigit():
            return int(s)
        else:
            return yaml.load(s)
    else:
        return s

def unzip():
    for root_no, dirs_no, files in os.walk('./huawei'):
        for name in files:
            print(name)
            if name.endswith('.zip') and name.startswith(current_date):
                with zipfile.ZipFile(os.path.join('./huawei', name), 'r') as zfile:
                    for _name in zfile.namelist():
                        extract_archiver(db, _name, zfile, current_date)
            elif name.endswith('.rar') and name.startswith(current_date):
                with rarfile.RarFile(os.path.join('./huawei', name), 'r') as rfile:
                    for _name in rfile.namelist():
                        extract_archiver(db, _name, rfile, current_date)

def extract_archiver(db, filename, parentzip, idate):
    if filename.endswith('.zip'):
        zfiledata = io.BytesIO(parentzip.read(filename))
        with zipfile.ZipFile(zfiledata) as zfile:
            for name in zfile.namelist():
                if name.endswith(tuple(extension)):
                    extract_archiver(db, name, zfile, idate)
    elif filename.endswith('.rar'):
        rfiledata = io.BytesIO(parentzip.read(filename))
        with rarfile.RarFile(rfiledata) as rfile:
            for name in rfile.namelist():
                if name.endswith(tuple(extension)):
                    extract_archiver(db, name, rfile, idate)
    elif filename.endswith('.gz'):
        print(filename)
        gfiledata = io.BytesIO(parentzip.read(filename))
        gfile = gzip.GzipFile(fileobj=gfiledata, mode='rb')
        tree = etree.iterparse(io.BytesIO(gfile.read()))
        for _, el in tree:
            el.tag = el.tag.split('}', 1)[1]    # # strip all namespaces
        root = tree.root
        hw_import(db, root, current_date)

def hw_import(db, root, idate):
    # 提取出基站eNodeBId
    eNodeBId = root.xpath('//eNodeBFunction/attributes')[0].find('eNodeBId').text
    # 提取出基站名称
    name = root.xpath('//eNodeBFunction/attributes')[0].find('eNodeBFunctionName').text
    # 以class作为一个table
    for item_class in root.iter('class'):
        # collections列表
        collections_list = []
        for item_collection in item_class.iterchildren():
            collection_dict = {}
            for item_attribute in item_collection.iterchildren():
                for item_field in item_attribute.iterchildren(tag=etree.Element):
                    # 判断是否位列表
                    if item_field.xpath('.//element'):
                        field_list = []
                        for item_element in item_field.iterchildren(tag=etree.Element):
                            for _field in item_element.iterchildren(tag=etree.Element):
                                field_list.append(isnumeric(str(_field.text).strip()))
                        collection_dict[item_field.tag] = field_list
                    else:
                        collection_dict[item_field.tag] = isnumeric(str(item_field.text).strip())
            if collection_dict:
                collection_dict.update({'iDate': idate, 'eNodeB_Id': isnumeric(eNodeBId), 'eNodeBId_Name': name})
                collections_list.append(collection_dict)
        if collections_list:
            db[item_collection.tag].insert_many(collections_list)

if __name__ == '__main__':
    unzip()
