#验证无索引、file级别索引、row group粒度索引的收益和开销
import datetime

import pyarrow as pa
import pyarrow.parquet as pp
import os
import sys
import matplotlib.pyplot as plt
import numpy as np
import time
from pybloom_live import ScalableBloomFilter, BloomFilter

dir_path = "/mydata/tpch_1000/lineitemsorted/"
files = os.listdir(dir_path)
files = [dir_path + file for file in files]
column = "receiptdate"
column_type = "date"
sys_min_value = sys.maxsize
sys_max_value = -sys.maxsize - 1
index_dir_path = "/mydata/tpch_1000/lineitemsortedindex/"

#对value1和value2获得其中的最小最大值
def get_min_max_value(column_type, value1, value2):
    if column_type == "int":
        _min = min(int(value1), int(value2))
        _max = max(int(value1), int(value2))
        return str(_min), str(_max)
    elif column_type == "date":
        value1 = datetime.date(int(value1.strip().split("-")[0]), int(value1.strip().split("-")[1]), int(value1.strip().split("-")[2]))
        value2 = datetime.date(int(value2.strip().split("-")[0]), int(value2.strip().split("-")[1]), int(value2.strip().split("-")[2]))
        _min = min(value1, value2)
        _max = max(value1, value2)
        _min = str(_min.year) + "-" + str(_min.month) + "-" + str(_min.day)
        _max = str(_max.year) + "-" + str(_max.month) + "-" + str(_max.day)
        return _min, _max

#不同类型的值获取初始最小最大值（字符串类型）
def get_sys_min_max_value(column_type):
    if column_type == "int":
        _min = sys.maxsize
        _max = -sys.maxsize - 1
        return str(_min), str(_max)
    elif column_type == "date":
        return "2099-1-1", "1-1-1"

def constructIndex():
    file_index_writer = open(index_dir_path + "fileindex.txt", "w+", encoding="UTF-8")
    for file in files:
        rg_index_writer = open(index_dir_path + file.split("/")[-1] + column, "w+", encoding="UTF-8")
        file_min_value, file_max_value = get_sys_min_max_value(column_type)
        _table = pp.ParquetFile(file)
        num_of_row_groups = _table.num_row_groups
        for row_group_index in range(num_of_row_groups):
            rg_min_value, rg_max_value = get_sys_min_max_value(column_type)
            row_group_contents = _table.read_row_group(row_group_index, columns=[column])
            for _ in row_group_contents.column(column):
                _ = str(_)
                file_min_value = get_min_max_value(column_type, _, file_min_value)[0]
                file_max_value = get_min_max_value(column_type, _, file_max_value)[1]
                rg_min_value = get_min_max_value(column_type, _, rg_min_value)[0]
                rg_max_value = get_min_max_value(column_type, _, rg_max_value)[1]
            rg_index_writer.write(file + " " + str(row_group_index) + " " + str(rg_min_value) + " " + str(rg_max_value) + "\n")
        file_index_writer.write(file + " " + str(file_min_value) + " " + str(file_max_value) + "\n")
        rg_index_writer.close()
    file_index_writer.close()

def getValueWithoutIndex(value):
    accord_num = 0
    file_num = 0
    for file in files:
        file_num += 1
        _table = pp.ParquetFile(file).read(columns=[column])
        for _ in _table.column(column):
            if int(str(_)) == value:
                accord_num += 1
    print("getValueWithoutIndex_accord_num:" + str(accord_num))
    print("getValueWithoutIndex_rg_num:" + str(file_num * 10))

def getValueWithFileIndex(value):
    accord_num = 0
    file_num = 0
    f = open(index_dir_path + "fileindex.txt", "r+", encoding="UTF-8")
    for line in f:
        file = line.split(" ")[0].strip()
        min_value = int(line.split(" ")[1].strip())
        max_value = int(line.split(" ")[2].strip())
        if(value > min_value and value < max_value):
            file_num += 1
            _table = pp.ParquetFile(file).read(columns=[column])
            for _ in _table.column(column):
                if int(str(_)) == value:
                    accord_num += 1
    f.close()
    print("getValueWithFileIndex_accord_num:" + str(accord_num))
    print("getValueWithFileIndex_rg_num:" + str(file_num * 10))

def getValueWithRGIndex(value):
    accord_num = 0
    rg_num = 0
    f = open(index_dir_path + "rgindex.txt", "r+", encoding="UTF-8")
    for line in f:
        file = line.split(" ")[0].strip()
        rg_index = int(line.split(" ")[1].strip())
        min_value = int(line.split(" ")[2].strip())
        max_value = int(line.split(" ")[3].strip())
        if (value > min_value and value < max_value):
            _table = pp.ParquetFile(file).read_row_group(rg_index, columns=[column])
            rg_num += 1
            for _ in _table.column(column):
                if int(str(_)) == value:
                    accord_num += 1
    f.close()
    print("getValueWithRGIndex_accord_num:" + str(accord_num))
    print("getValueWithRGIndex_rg_num:" + str(rg_num))



constructIndex()
# search_value = 25128964
# pre_time = int(round(time.time()))
# getValueWithoutIndex(search_value)
# print("withoutindex_time:" + str(int(round(time.time())) - pre_time))
# pre_time = int(round(time.time()))
# getValueWithFileIndex(search_value)
# print("withfileindex_time:" + str(int(round(time.time())) - pre_time))
# pre_time = int(round(time.time()))
# getValueWithRGIndex(search_value)
# print("withrgwindex_time:" + str(int(round(time.time())) - pre_time))
# pre_time = int(round(time.time()))