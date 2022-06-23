#基于jaccard进行测试
#jaccard系数小：直接构建倒排索引
#jaccard系数时钟：min-max + bloom
#jaccard系数大：不构建索引

import datetime
import os
import sys

import pyarrow.parquet as pp
from pybloom_live import ScalableBloomFilter, BloomFilter

from iouCompute import com_iou, com_iou_random
from jaccardCompute import com_jaccard, com_jaccard_random


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

def get_max(column_type, value1, value2):
    if column_type == "date":
        temp_value1 = datetime.date(int(value1.split("-")[0]), int(value1.split("-")[1]), int(value1.split("-")[2]))
        temp_value2 = datetime.date(int(value2.split("-")[0]), int(value2.split("-")[1]), int(value2.split("-")[2]))
    elif column_type == "int":
        temp_value1 = int(value1)
        temp_value2 = int(value2)
    elif column_type == "float":
        temp_value1 = float(value1)
        temp_value2 = float(value2)
    if temp_value1 >= temp_value2:
        return value1
    else:
        return value2

def get_min(column_type, value1, value2):
    if column_type == "date":
        temp_value1 = datetime.date(int(value1.split("-")[0]), int(value1.split("-")[1]), int(value1.split("-")[2]))
        temp_value2 = datetime.date(int(value2.split("-")[0]), int(value2.split("-")[1]), int(value2.split("-")[2]))
    elif column_type == "int":
        temp_value1 = int(value1)
        temp_value2 = int(value2)
    elif column_type == "float":
        temp_value1 = float(value1)
        temp_value2 = float(value2)
    if temp_value1 <= temp_value2:
        return value1
    else:
        return value2

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
    _all_iou = 0
    file_count = 0
    for file in files:
        if ".png" in file:
            continue
        if file_count > 1:
            break
        file_count += 1
        _all_jaccard += com_jaccard_random(_dir + table + "/" + file, column, column_type)
        _all_iou += com_iou_random(_dir + table + "/" + file, column, column_type)[0]
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
    return _valid_block_num, _all_block_num, _all_jaccard/file_count, _all_iou/file_count

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
                if is_lt(column_type, get_max(column_type, _min, _start), get_min(column_type, _max, _end)):
                    _valid_block_num += 1
    return _valid_block_num

if __name__ == "__main__":
    search_list = [
        ["/mydata/tpch_parquet_300.db_rewrite/", "lineitem", "orderkey", "int", "1000000000", "1035000000", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db_rewrite/", "lineitem", "partkey", "int", "30000000", "31200000", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db_rewrite/", "lineitem", "extendedprice", "float", "40000", "42000", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db_rewrite/", "part", "partkey", "int", "30000000", "31200000", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db_rewrite/", "customer", "custkey", "int", "30000000", "30900000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_returns", "cr_order_number", "int", "30000000", "31000000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_returns", "cr_returning_addr_sk", "int", "1000000", "1050000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_sales", "cs_sold_date_sk", "int", "2451000", "2451060", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_sales", "cs_sold_date_sk", "int", "2451100", "2451160", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_sales", "cs_sold_date_sk", "int", "2451300", "2451360", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_sales", "cs_sold_date_sk", "int", "2451700", "2451760", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_sales", "cs_order_number", "int", "2451000", "2451060", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_sales", "cs_catalog_page_sk", "int", "10000", "10400", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "customer", "c_customer_sk", "int", "2000000", "2100000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "customer_address", "ca_address_sk", "int", "1000000", "1050000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "customer_demographics", "cd_demo_sk", "int", "1000000", "1040000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "store_sales", "ss_item_sk", "int", "100000", "105280", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "store_sales", "ss_customer_sk", "int", "2000000", "2100000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "store_sales", "ss_addr_sk", "int", "1000000", "1050000", "范围查询2%"],

        ["/mydata/tpch_parquet_300.db_rewrite/", "lineitem", "orderkey", "int", "1000000000", "1000000000", "点查询"],
        ["/mydata/tpch_parquet_300.db_rewrite/", "lineitem", "partkey", "int", "30000000", "30000000", "点查询"],
        ["/mydata/tpch_parquet_300.db_rewrite/", "part", "partkey", "int", "30000000", "30000000", "点查询"],
        ["/mydata/tpch_parquet_300.db_rewrite/", "customer", "custkey", "int", "30000000", "30000000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_returns", "cr_order_number", "int", "30000000", "30000000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_returns", "cr_returning_addr_sk", "int", "1000000", "1000000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_sales", "cs_sold_date_sk", "int", "2451000", "2451000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_sales", "cs_order_number", "int", "2451000", "2451000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "catalog_sales", "cs_catalog_page_sk", "int", "10000", "10000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "customer", "c_customer_sk", "int", "2000000", "2000000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "customer_address", "ca_address_sk", "int", "1000000", "1000000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "customer_demographics", "cd_demo_sk", "int", "1000000", "1000000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "customer_demographics", "cd_dep_count", "int", "0", "0", "点查询%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "store_sales", "ss_item_sk", "int", "100000", "100000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "store_sales", "ss_customer_sk", "int", "2000000", "2000000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db_rewrite/", "store_sales", "ss_addr_sk", "int", "1000000", "1000000", "点查询%"]
    ]
    origin_search_list = [
        ["/mydata/tpch_parquet_300.db/", "lineitem", "orderkey", "int", "1000000000", "1035000000", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db/", "lineitem", "partkey", "int", "30000000", "31200000", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db/", "lineitem", "extendedprice", "float", "40000", "42000", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db/", "part", "partkey", "int", "30000000", "31200000", "范围查询2%"],
        ["/mydata/tpch_parquet_300.db/", "customer", "custkey", "int", "30000000", "30900000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db/", "catalog_returns", "cr_order_number", "int", "30000000", "31000000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db/", "catalog_returns", "cr_returning_addr_sk", "int", "1000000", "1050000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db/", "catalog_sales", "cs_sold_date_sk", "int", "2451000", "2451060", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db/", "catalog_sales", "cs_order_number", "int", "2451000", "2451060", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db/", "catalog_sales", "cs_catalog_page_sk", "int", "10000", "10400", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db/", "customer", "c_customer_sk", "int", "2000000", "2100000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db/", "customer_address", "ca_address_sk", "int", "1000000", "1050000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db/", "customer_demographics", "cd_demo_sk", "int", "1000000", "1040000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db/", "store_sales", "ss_item_sk", "int", "100000", "105280", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db/", "store_sales", "ss_customer_sk", "int", "2000000", "2100000", "范围查询2%"],
        ["/mydata/tpcds_parquet_300.db/", "store_sales", "ss_addr_sk", "int", "1000000", "1050000", "范围查询2%"],

        ["/mydata/tpch_parquet_300.db/", "lineitem", "orderkey", "int", "1000000000", "1000000000", "点查询"],
        ["/mydata/tpch_parquet_300.db/", "lineitem", "partkey", "int", "30000000", "30000000", "点查询"],
        ["/mydata/tpch_parquet_300.db/", "part", "partkey", "int", "30000000", "30000000", "点查询"],
        ["/mydata/tpch_parquet_300.db/", "customer", "custkey", "int", "30000000", "30000000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db/", "catalog_returns", "cr_order_number", "int", "30000000", "30000000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db/", "catalog_returns", "cr_returning_addr_sk", "int", "1000000", "1000000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db/", "catalog_sales", "cs_sold_date_sk", "int", "2451000", "2451000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db/", "catalog_sales", "cs_order_number", "int", "2451000", "2451000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db/", "catalog_sales", "cs_catalog_page_sk", "int", "10000", "10000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db/", "customer", "c_customer_sk", "int", "2000000", "2000000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db/", "customer_address", "ca_address_sk", "int", "1000000", "1000000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db/", "customer_demographics", "cd_demo_sk", "int", "1000000", "1000000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db/", "customer_demographics", "cd_dep_count", "int", "0", "0", "点查询%"],
        ["/mydata/tpcds_parquet_300.db/", "store_sales", "ss_item_sk", "int", "100000", "100000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db/", "store_sales", "ss_customer_sk", "int", "2000000", "2000000", "点查询%"],
        ["/mydata/tpcds_parquet_300.db/", "store_sales", "ss_addr_sk", "int", "1000000", "1000000", "点查询%"]
    ]
    for i in range(7, 12):
        search = search_list[i]
        perfectnum, allnum, jaccard, iou = naiveSearch(search[0], search[1], search[2], search[3], search[4], search[5])
        mmbnum = searchWithMMB(search[0], search[1], search[2], search[3], search[4], search[5])
        print(str(search) + " jaccard:" + str(jaccard) + " iou:" + str(iou) + " " + "perfectnum:" + str(perfectnum) + " " + "allnum:" + str(allnum) + " " + "mmbnum:" + str(mmbnum))
    print("origin_search")
    for i in range(7, 12):
        search = origin_search_list[i]
        perfectnum, allnum, jaccard, iou = naiveSearch(search[0], search[1], search[2], search[3], search[4], search[5])
        search.append(jaccard)
        print(str(search) + " jaccard:" + str(jaccard) + " iou:" + str(iou) + " " + "perfectnum:" + str(
            perfectnum) + " " + "allnum:" + str(allnum) + " " + "mmbnum:" + str(mmbnum))
