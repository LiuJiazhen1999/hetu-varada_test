#验证无索引、file级别索引、row group粒度索引的收益和开销
import datetime

import pyarrow.parquet as pp
import os
import sys

#对value1和value2获得其中的最小最大值
def get_min_max_value(column_type, value1, value2):
    if column_type == "int":
        _min = min(int(value1), int(value2))
        _max = max(int(value1), int(value2))
        return str(_min), str(_max)
    elif column_type == "float":
        _min = min(float(value1), float(value2))
        _max = max(float(value1), float(value2))
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
    elif column_type == "float":
        _min = sys.maxsize
        _max = -sys.maxsize - 1
        return str(_min), str(_max)

#创建类似ORC的min-max索引，包括文件级别和row group粒度
def constructORCLikeIndex(orc_index_dir_path, files, _columns, _columns_type):
    for file in files:
        _table = pp.ParquetFile(file)
        num_of_row_groups = _table.num_row_groups
        for row_group_index in range(num_of_row_groups):
            row_group_contents = _table.read_row_group(row_group_index, columns=_columns)
            for _index in range(len(_columns)):
                _column = _columns[_index]
                _column_type = _columns_type[_index]
                rg_min_value, rg_max_value = get_sys_min_max_value(_column_type)
                rg_index_writer = open(orc_index_dir_path + file.split("/")[-1] + "_" + _column, "a+", encoding="UTF-8")
                for _ in row_group_contents.column(_column):
                    _ = str(_)
                    rg_min_value = get_min_max_value(_column_type, _, rg_min_value)[0]
                    rg_max_value = get_min_max_value(_column_type, _, rg_max_value)[1]
                rg_index_writer.write(file + " " + str(row_group_index) + " " + str(rg_min_value) + " " + str(rg_max_value) + "\n")
                rg_index_writer.close()

def isValueIn(line, predict_express):
    _datetype = predict_express[2]
    _filtertype = predict_express[1]
    if _filtertype == "range":
        if _datetype == "date":
            _searpredate = datetime.date(int(predict_express[3].split("-")[0]), int(predict_express[3].split("-")[1]), int(predict_express[3].split("-")[2]))
            _searenddate = datetime.date(int(predict_express[4].split("-")[0]), int(predict_express[4].split("-")[1]), int(predict_express[4].split("-")[2]))
            _filepredate = datetime.date(int(line.split(" ")[2].split("-")[0]), int(line.split(" ")[2].split("-")[1]), int(line.split(" ")[2].split("-")[2]))
            _fileenddate = datetime.date(int(line.split(" ")[3].split("-")[0]), int(line.split(" ")[3].split("-")[1]), int(line.split(" ")[3].split("-")[2]))
            return (not (max(_searpredate, _filepredate) < min(_searenddate, _fileenddate)))


#在上述ORC-min-max索引的基础上构建谓词索引
def constructPredicateIndex(orc_index_dir_path, predict_index_dir_path):
    predict_express = ["receiptdate", "range", "date", "1994-01-01", "1995-01-01"]#谓词表达式，分别是列名、查询类型、列类型、最小值、最大值（或者点查的就精确值）
    orc_index_files = os.listdir(orc_index_dir_path)
    predict_index_writer = open(predict_index_dir_path + value for value in predict_express)
    for orc_index_file in orc_index_files:
        if predict_express[0] in orc_index_file:
            index_reader = open(orc_index_file, "r+", encoding="UTF-8")
            for line in index_reader:
                if isValueIn(line, predict_express):
                    predict_index_writer.write(line.split(" ")[0] + line.split(" ")[1] + "\n")
            index_reader.close()
    predict_index_writer.close()


if __name__ == "__main__":
    file_dir = "/mydata/tpch_parquet_300.db_rewrite"
    index_dir = file_dir + "_index/"
    file_dir += "/"
    _tables = os.listdir(file_dir)
    tpch_table_set = {"lineitem": [["orderkey", "partkey", "suppkey", "linenumber", "quantity", "extendedprice", "discount", "tax", "shipdate", "commitdate", "receiptdate"], ["int", "int", "int", "int", "float", "float", "float", "float", "date", "date", "date"]],
                      "part": [["partkey", "size", "retailprice"], ["int", "int", "float"]],
                      "supplier": [["suppkey", "nationkey", "acctbal"], ["int", "int", "float"]],
                      "partsupp": [["partkey", "suppkey", "availqty", "supplycost"], ["int", "int", "int", "float"]],
                      "customer": [["custkey", "nationkey", "acctbal"], ["int", "int", "float"]],
                      "order": [["orderkey", "custkey", "totalprice", "orderdate", "shippriority"], ["int", "int", "float", "date", "int"]]}
    for _table in _tables:
        if not os.path.exists(index_dir + _table):
            os.makedirs(index_dir + _table)
        _files = os.listdir(file_dir + _table)
        _files = [file_dir + _table + "/" + file for file in _files]
        constructORCLikeIndex(index_dir + _table + "/", _files, tpch_table_set.get(_table)[0], tpch_table_set.get(_table)[1])