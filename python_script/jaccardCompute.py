#随机抽取文件中的两个row group，计算两者之间的Jaccard作为文件的数据分布情况

import random

import numpy as np
import pyarrow.parquet as pp
from scipy.spatial.distance import pdist


def get_2_diff_index(row_group_num):
    random1 = random.random()
    random2 = random.random()
    while random1 == random2:
        random2 = random.random()
    random1 = int(random1 * row_group_num)
    random2 = int(random2 * row_group_num)
    assert random1 >= 0 and random1 < row_group_num
    assert random2 >=0 and random2 < row_group_num
    return random1, random2

def com_jaccard_np(file_path, column_name, column_type):
    _table = pp.ParquetFile(file_path)
    rg_num = _table.num_row_groups
    if rg_num < 10:
        return 0
    arr1 = np.array([])
    arr2 = np.array([])
    for rg_index in range(0, int(rg_num / 5)):
        if rg_index % 2 == 0:
            rg1_content = _table.read_row_group(rg_index, columns=[column_name])
            for _ in rg1_content.column(column_name):
                _ = str(_)
                if _ == "None":
                    continue
                if column_type == "int":
                    _ = int(_)
                elif column_type == "float":
                    _ = float(_)
                arr1 = np.append(arr1, _)
        if rg_index % 2 == 1:
            rg2_content = _table.read_row_group(rg_index, columns=[column_name])
            for _ in rg2_content.column(column_name):
                _ = str(_)
                if _ == "None":
                    continue
                if column_type == "int":
                    _ = int(_)
                elif column_type == "float":
                    _ = float(_)
                arr2 = np.append(arr2, _)
    _len = min(arr1.size, arr2.size)
    X = np.vstack([arr1[0:_len], arr2[0:_len]])
    return pdist(X, 'jaccard')

def com_jaccard_random(file_path, column_name, column_type):
    _table = pp.ParquetFile(file_path)
    rg_num = _table.num_row_groups
    if rg_num < 10:
        return 0
    union_num = 0
    join_num = 0
    rg1_dict = dict()
    _sample_num = int(rg_num/5)
    if _sample_num % 2 == 1:
        _sample_num += 1
    sample_set = set()
    while len(sample_set) < _sample_num:
        sample_set.add(random.randint(0, rg_num-1))
    cur_index = 0
    for _index in sample_set:
        if cur_index % 2 == 0:
            rg_content = _table.read_row_group(_index, columns=[column_name])
            for _ in rg_content.column(column_name):
                _ = str(_)
                if _ == "None":
                    continue
                if column_type == "int":
                    _ = int(_)
                elif column_type == "float":
                    _ = float(_)
                union_num += 1
                if _ not in rg1_dict:
                    rg1_dict[_] = 0
                rg1_dict[_] += 1
        cur_index += 1
    cur_index = 0
    for _index in sample_set:
        if cur_index % 2 == 1:
            rg_content = _table.read_row_group(_index, columns=[column_name])
            for _ in rg_content.column(column_name):
                _ = str(_)
                if _ == "None":
                    continue
                if column_type == "int":
                    _ = int(_)
                elif column_type == "float":
                    _ = float(_)
                union_num += 1
                if _ in rg1_dict:
                    join_num += (1 + rg1_dict[_])
                    rg1_dict[_] = 0
        cur_index += 1
    return join_num / union_num


def com_jaccard(file_path, column_name, column_type):
    rg1_dict = dict()
    union_num = 0
    join_num = 0
    _table = pp.ParquetFile(file_path)
    rg_num = _table.num_row_groups
    if rg_num < 10:
        return 0
    for rg_index1 in range(0, int(rg_num/5)):
        if rg_index1 % 2 == 0:
            rg1_content = _table.read_row_group(rg_index1, columns=[column_name])
            for _ in rg1_content.column(column_name):
                _ = str(_)
                if _ == "None":
                    continue
                if column_type == "int":
                    _ = int(_)
                elif column_type == "float":
                    _ = float(_)
                union_num += 1
                if _ not in rg1_dict:
                    rg1_dict[_] = 0
                rg1_dict[_] += 1
    for rg_index2 in range(0, int(rg_num/5)):
        if rg_index2 % 2 == 1:
            rg2_content = _table.read_row_group(rg_index2, columns=[column_name])
            for _ in rg2_content.column(column_name):
                _ = str(_)
                if _ == "None":
                    continue
                if column_type == "int":
                    _ = int(_)
                elif column_type == "float":
                    _ = float(_)
                union_num += 1
                if _ in rg1_dict:
                    join_num += (1 + rg1_dict[_])
                    rg1_dict[_] = 0
    return join_num/union_num

def get_file_rg_num(file_path):
    return pp.ParquetFile(file_path).num_row_groups

if __name__=="__main__":
    file_path = "/mydata/tpch_parquet_300.db_rewrite/orders/20220524_062839_00026_fs53m_66cba92c-a213-4e48-ab15-5b37a5bb0daa"
    column_name = "orderdate"
    column_type = "date"
    _jaccard = com_jaccard(file_path, column_name, column_type)
    print(file_path + "~" + column_name + "~" + str(_jaccard))  # 这个percent是1.0

    # file_path = "/mydata/tpcds_parquet_300.db_rewrite/catalog_returns/20220523_132505_00004_fs53m_509e9f99-1d58-4cf7-9d90-98845500e399"
    # column_name = "cr_item_sk"
    # column_type = "float"
    # _jaccard = com_jaccard(file_path, column_name, column_type)
    # print(file_path + "~" + column_name + "~" + str(_jaccard))#这个percent是1.0
    #
    # column_name = "cr_returned_date_sk"
    # _jaccard = com_jaccard(file_path, column_name, column_type)
    # print(file_path + "~" + column_name + "~" + str(_jaccard))  # 这个percent未知，看起来在1/4左右
    #
    # file_path = "/mydata/tpcds_parquet_300.db_rewrite/catalog_sales/20220523_132919_00005_fs53m_ff916c89-b7ca-4b23-b0f1-efd34a8ec887"
    # column_name = "cs_net_paid_inc_ship"
    # _jaccard = com_jaccard(file_path, column_name, column_type)
    # print(file_path + "~" + column_name + "~" + str(_jaccard))  # 这个percent是0.5
    #
    # file_path = "/mydata/tpch_parquet_300.db_rewrite/lineitem/20220524_040257_00019_fs53m_0ac223b1-073d-419a-a423-a83527104d5b"
    # column_name = "extendedprice"
    # _jaccard = com_jaccard(file_path, column_name, column_type)
    # print(file_path + "~" + column_name + "~" + str(_jaccard))  # 这个percent是0.75
    #
    #
    # column_name = "shipdate"
    # _jaccard = com_jaccard(file_path, column_name, "date")
    # print(file_path + "~" + column_name + "~" + str(_jaccard))  # 这个percent是0.71
    #
    # file_path = "/mydata/tpch_parquet_300.db_rewrite/customer/20220524_064704_00027_fs53m_1af48bb7-7f26-4364-8cca-751850202710"
    # column_name = "custkey"
    # _jaccard = com_jaccard(file_path, column_name, column_type)
    # print(file_path + "~" + column_name + "~" + str(_jaccard))  # 这个percent是0.004