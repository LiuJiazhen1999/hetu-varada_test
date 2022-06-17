#基于jaccard进行测试
#jaccard系数小：直接构建倒排索引
#jaccard系数时钟：min-max + bloom
#jaccard系数大：不构建索引

import datetime
import os
import sys

import pyarrow.parquet as pp
from pybloom_live import ScalableBloomFilter, BloomFilter

from jaccardCompute import com_jaccard


def is_lt(column_type, value1, value2):
    """
    :param column_type: 值的类型
    :param value1:
    :param value2:
    :return: value1 <= value2
    """
    if(column_type == "date"):
        return datetime.date(int(value1.split("-")[0]), int(value1.split("-")[1]), int(value1.split("-")[2])) <= datetime.date(int(value2.split("-")[0]), int(value2.split("-")[1]), int(value2.split("-")[2]))
    else:
        return float(value1) <= float(value2)

def is_gt(column_type, value1, value2):
    """
    :param column_type: 值的类型
    :param value1:
    :param value2:
    :return: value1 >= value2
    """
    if(column_type == "date"):
        return datetime.date(int(value1.split("-")[0]), int(value1.split("-")[1]),
                             int(value1.split("-")[2])) >= datetime.date(int(value2.split("-")[0]),
                                                                         int(value2.split("-")[1]),
                                                                         int(value2.split("-")[2]))
    else:
        return float(value1) >= float(value2)

def is_eq(column_type, value1, value2):
    """
    :param column_type:值的类型
    :param value1:
    :param value2:
    :return: value1 == value2
    """
    if (column_type == "date"):
        return datetime.date(int(value1.split("-")[0]), int(value1.split("-")[1]),
                             int(value1.split("-")[2])) == datetime.date(int(value2.split("-")[0]),
                                                                         int(value2.split("-")[1]),
                                                                         int(value2.split("-")[2]))
    else:
        return float(value1) == float(value2)

def get_sys_min_max_value(column_type):
    """
    :param column_type: 值的类型
    :return:不同类型的值获取初始最小最大值（字符串类型）
    """
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

def naiveSearch(_dir, table, column, column_type, _start, _end):
    """
    返回值：int类型，表示真实符合搜索条件[_start, _end]的块数; 以及不带索引搜索的块数
    :param _dir: table所在的文件夹的绝对路径
    :param table: 搜索的表
    :param column: 搜索的列
    :param column_type: 搜索列的类型
    :param _start: 搜索的起始范围
    :param _end: 搜索的终止范围
    """
    files = os.listdir(_dir + table)
    files.sort()
    _valid_block_num = 0
    _all_block_num = 0
    _all_jaccard = 0
    file_count = 0
    for file in files:
        if ".png" in file:
            continue
        if file_count > 1:
            break
        file_count += 1
        _all_jaccard += com_jaccard(_dir + table + "/" + file, column, column_type)
        _table = pp.ParquetFile(_dir + table + "/" + file)
        num_of_row_groups = _table.num_row_groups
        _all_block_num += num_of_row_groups
        for _index in range(num_of_row_groups):
            row_group_contents = _table.read_row_group(_index, columns=[column])
            for _value in row_group_contents.column(column):
                _value = str(_value)
                if _value == "None":
                    continue
                if is_lt(column_type, _start, _value) and is_gt(column_type, _end, _value):
                    _valid_block_num += 1
                    break
    return _valid_block_num, _all_block_num, _all_jaccard/file_count

def searchWithMMB(_dir, table, column, column_type, _start, _end):
    """
    返回值：int类型，表示符合搜索条件[_start, _end]的块数
    :param _dir: table所在的文件夹的绝对路径
    :param table: 搜索的表
    :param column: 搜索的列
    :param column_type: 搜索的列的类型
    :param _start: 搜索的起始范围
    :param _end: 搜索的终止范围
    """
    files = os.listdir(_dir + table)
    files.sort()
    _valid_block_num = 0
    file_count = 0
    for file in files:
        if ".png" in file:
            continue
        if file_count > 1:
            break
        file_count += 1
        _table = pp.ParquetFile(_dir + table + "/" + file)
        num_of_row_groups = _table.num_row_groups
        for _index in range(num_of_row_groups):
            row_group_contents = _table.read_row_group(_index, columns=[column])
            _min, _max = get_sys_min_max_value(column_type)
            bloom = ScalableBloomFilter(initial_capacity=1000, error_rate=0.001)
            for _value in row_group_contents.column(column):
                _value = str(_value)
                if _value == "None":
                    continue
                if is_lt(column_type, _value, _min):
                    _min = _value
                if is_gt(column_type, _value, _max):
                    _max = _value
                if column_type == "float":
                    _value = str(float(_value))
                bloom.add(_value)
            if _start == _end:#点查询
                if column_type == "float":
                    _start = str(float(_start))
                if _start in bloom:
                    _valid_block_num += 1
            else:#范围查询
                if (is_lt(column_type, _start, _max) and is_gt(column_type, _start, _min)) or (is_lt(column_type, _end, _max) and is_gt(column_type, _end, _min)):
                    _valid_block_num += 1
    return _valid_block_num

if __name__ == "__main__":
    search_list = [
        ["/mydata/tpch_parquet_300.db_rewrite/", "lineitem", "partkey", "int", "40000000", "41200000", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db_rewrite/", "lineitem", "suppkey", "int", "2000000", "2060000", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db_rewrite/", "lineitem", "extendedprice", "float", "50000", "52000", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db_rewrite/", "lineitem", "shipdate", "date", "1995-1-1", "1995-2-20", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db_rewrite/", "lineitem", "partkey", "int", "40000000", "40000000", "点查询"],
        ["/mydata/tpch_parquet_300.db_rewrite/", "lineitem", "suppkey", "int", "2000000", "2000000", "点查询"],
        ["/mydata/tpch_parquet_300.db_rewrite/", "lineitem", "extendedprice", "float", "50000", "50000", "点查询"],
        ["/mydata/tpch_parquet_300.db_rewrite/", "lineitem", "shipdate", "date", "1995-1-1", "1995-1-1", "点查询"],
        ["/mydata/tpch_parquet_300.db_rewrite/", "customer", "custkey", "int", "20000000", "20800000", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db_rewrite/", "customer", "acctbal", "float", "5000", "5200", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db_rewrite/", "orders", "totalprice", "float", "200000", "212000", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db_rewrite/", "customer", "custkey", "int", "20000000", "20000000", "点查询"],
        ["/mydata/tpch_parquet_300.db_rewrite/", "customer", "acctbal", "float", "5000", "5000", "点查询"],
        ["/mydata/tpch_parquet_300.db_rewrite/", "orders", "totalprice", "float", "200000", "200000", ""],#14
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_returns", "cr_refunded_customer_sk", "int", "2000000", "2100000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_returns", "cr_refunded_cdemo_sk", "int", "1000000", "1040000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_returns", "cr_returning_customer_sk", "int", "2000000", "2100000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_returns", "cr_returning_addr_sk", "int", "1000000", "1050000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_returns", "cr_refunded_customer_sk", "int", "2000000", "2000000", "点查询"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_returns", "cr_refunded_cdemo_sk", "int", "1000000", "1000000", "点查询"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_returns", "cr_returning_customer_sk", "int", "2000000", "2000000", "点查询"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_returns", "cr_returning_addr_sk", "int", "1000000", "1000000", "点查询"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "store_sales", "ss_customer_sk", "int", "1000000", "1040000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "store_sales", "ss_cdemo_sk", "int", "1000000", "1100000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "store_sales", "ss_customer_sk", "int", "1000000", "1000000", "点查询"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "store_sales", "ss_cdemo_sk", "int", "2000000", "2000000", "点查询"]
    ]
    origin_search_list = [
        ["/mydata/tpch_parquet_300.db/", "lineitem", "partkey", "int", "40000000", "41200000", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db/", "lineitem", "suppkey", "int", "2000000", "2060000", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db/", "lineitem", "extendedprice", "float", "50000", "52000", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db/", "lineitem", "shipdate", "date", "1995-1-1", "1995-2-20", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db/", "lineitem", "partkey", "int", "40000000", "40000000", "点查询"],
        ["/mydata/tpch_parquet_300.db/", "lineitem", "suppkey", "int", "2000000", "2000000", "点查询"],
        ["/mydata/tpch_parquet_300.db/", "lineitem", "extendedprice", "float", "50000", "50000", "点查询"],
        ["/mydata/tpch_parquet_300.db/", "lineitem", "shipdate", "date", "1995-1-1", "1995-1-1", "点查询"],
        ["/mydata/tpch_parquet_300.db/", "customer", "custkey", "int", "20000000", "20800000", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db/", "customer", "acctbal", "float", "5000", "5200", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db/", "orders", "totalprice", "float", "200000", "212000", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db/", "customer", "custkey", "int", "20000000", "20000000", "点查询"],
        ["/mydata/tpch_parquet_300.db/", "customer", "acctbal", "float", "5000", "5000", "点查询"],
        ["/mydata/tpch_parquet_300.db/", "orders", "totalprice", "float", "200000", "200000", "点查询"],#14
        ["/mydata/tpcds_parquet_300.db/", "catalog_returns", "cr_refunded_customer_sk", "int", "2000000", "2100000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db/", "catalog_returns", "cr_refunded_cdemo_sk", "int", "1000000", "1040000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db/", "catalog_returns", "cr_returning_customer_sk", "int", "2000000", "2100000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db/", "catalog_returns", "cr_returning_addr_sk", "int", "1000000", "1050000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db/", "catalog_returns", "cr_refunded_customer_sk", "int", "2000000", "2000000", "点查询"],
        ["/mydata/tpcds_parquet_300.db/", "catalog_returns", "cr_refunded_cdemo_sk", "int", "1000000", "1000000", "点查询"],
        ["/mydata/tpcds_parquet_300.db/", "catalog_returns", "cr_returning_customer_sk", "int", "2000000", "2000000", "点查询"],
        ["/mydata/tpcds_parquet_300.db/", "catalog_returns", "cr_returning_addr_sk", "int", "1000000", "1000000", "点查询"],
        ["/mydata/tpcds_parquet_300.db/", "store_sales", "ss_customer_sk", "int", "1000000", "1040000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db/", "store_sales", "ss_cdemo_sk", "int", "1000000", "1100000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db/", "store_sales", "ss_customer_sk", "int", "1000000", "1000000", "点查询"],
        ["/mydata/tpcds_parquet_300.db/", "store_sales", "ss_cdemo_sk", "int", "2000000", "2000000", "点查询"]
    ]
    for i in range(23, len(search_list)):
        search = search_list[i]
        perfectnum, allnum, jaccard = naiveSearch(search[0], search[1], search[2], search[3], search[4], search[5])
        mmbnum = searchWithMMB(search[0], search[1], search[2], search[3], search[4], search[5])
        search.append(jaccard)
        print(str(search) + " " + "perfectnum:" + str(perfectnum) + " " + "allnum:" + str(allnum) + " " + "mmbnum:" + str(mmbnum))
    print("origin_search")
    for i in range(23, len(origin_search_list)):
        search = origin_search_list[i]
        perfectnum, allnum, jaccard = naiveSearch(search[0], search[1], search[2], search[3], search[4], search[5])
        search.append(jaccard)
        print(str(search) + " " + "perfectnum:" + str(perfectnum) + " " + "allnum:" + str(allnum))