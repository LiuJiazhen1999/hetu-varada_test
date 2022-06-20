import datetime
import os
import pyarrow.parquet as pp
import matplotlib.pyplot as plt

from jaccardCompute import com_jaccard
from indexConstruct import get_sys_min_max_value, get_min_max_value

def is_lt(column_type, value1, value2):
    if(column_type == "date"):
        return datetime.date(int(value1.split("-")[0]), int(value1.split("-")[1]), int(value1.split("-")[2])) <= datetime.date(int(value2.split("-")[0]), int(value2.split("-")[1]), int(value2.split("-")[2]))
    else:
        return float(value1) <= float(value2)

def is_gt(column_type, value1, value2):
    if(column_type == "date"):
        return datetime.date(int(value1.split("-")[0]), int(value1.split("-")[1]),
                             int(value1.split("-")[2])) >= datetime.date(int(value2.split("-")[0]),
                                                                         int(value2.split("-")[1]),
                                                                         int(value2.split("-")[2]))
    else:
        return float(value1) >= float(value2)

def is_eq(column_type, value1, value2):
    if (column_type == "date"):
        return datetime.date(int(value1.split("-")[0]), int(value1.split("-")[1]),
                             int(value1.split("-")[2])) == datetime.date(int(value2.split("-")[0]),
                                                                         int(value2.split("-")[1]),
                                                                         int(value2.split("-")[2]))
    else:
        return float(value1) == float(value2)

def draw_scatter(file, dot_size, column, column_type, _start, _end):
    _jaccard = com_jaccard(file, column, column_type)
    _table = pp.ParquetFile(file)
    num_of_row_groups = _table.num_row_groups
    x1 = []#(x1, y1)单个row group的(_min, _max)，浅色
    y1 = []
    x2 = []#(x2, y2)符合条件的row group的(_min, _max)，深色
    y2 = []
    for row_group_index in range(num_of_row_groups):
        row_group_contents = _table.read_row_group(row_group_index, columns=[column])
        _min, _max = get_sys_min_max_value(column_type)
        _flag = False
        for _ in row_group_contents.column(column):
            if str(_) == 'None':
                continue
            _min = get_min_max_value(column_type, str(_), _min)[0]
            _max = get_min_max_value(column_type, str(_), _max)[1]
            if _start == _end:
                if(is_eq(column_type, _start, str(_))):
                    _flag = True
            else:
                if is_lt(column_type, _start, str(_)) and is_gt(column_type, _end, str(_)):
                    _flag = True
        if(column_type == "date"):
            _x = datetime.date(int(_min.split("-")[0]), int(_min.split("-")[1]), int(_min.split("-")[2]))
            _y = datetime.date(int(_max.split("-")[0]), int(_max.split("-")[1]), int(_max.split("-")[2]))
        else:
            _x = float(_min)
            _y = float(_max)
        if _flag:
            x2.append(_x)
            y2.append(_y)
        else:
            x1.append(_x)
            y1.append(_y)
        #print(str(row_group_index) + "-" + str(_min) + "-" + str(_max))
    fig = plt.figure()
    ax1 = fig.add_subplot(1, 1, 1)
    ax1.set_title("Fit percent:" + str(len(x2)/(len(x1) + len(x2))) + " jaccard:" + str(_jaccard))
    ax1.set_xlabel('min-value')
    ax1.set_ylabel('max-value')
    ax1.scatter(x1, y1, s=dot_size, c='lightgrey', marker='.')
    ax1.scatter(x2, y2, s=dot_size, c='k', marker='.')
    plt.savefig(file.split("/")[-2] + "_" + column + "_" + _start + "-" + _end + ".png")
    #plt.show()
    plt.close()

if __name__ == "__main__":
    file_dir = "/mydata/tpch_parquet_300.db_rewrite/lineitem/"
    files = os.listdir(file_dir)
    for file in files:
        if ".png" not in file:
            file_name = file
            break
    draw_scatter(file_dir + file_name, 10, "orderkey", "int", str("0"), str("0"))
    draw_scatter(file_dir + file_name, 10, "partkey", "int", str("0"), str("0"))

    file_dir = "/mydata/tpch_parquet_300.db_rewrite/part/"
    files = os.listdir(file_dir)
    for file in files:
        if ".png" not in file:
            file_name = file
            break
    draw_scatter(file_dir + file_name, 10, "partkey", "int", str("0"), str("0"))

    file_dir = "/mydata/tpch_parquet_300.db_rewrite/customer/"
    files = os.listdir(file_dir)
    for file in files:
        if ".png" not in file:
            file_name = file
            break
    draw_scatter(file_dir + file_name, 10, "custkey", "int", str("0"), str("0"))

    file_dir = "/mydata/tpcds_parquet_300.db_rewrite/catalog_returns/"
    files = os.listdir(file_dir)
    for file in files:
        if ".png" not in file:
            file_name = file
            break
    draw_scatter(file_dir + file_name, 10, "cr_order_number", "int", str("0"), str("0"))
    draw_scatter(file_dir + file_name, 10, "cr_returning_addr_sk", "int", str("0"), str("0"))
    draw_scatter(file_dir + file_name, 10, "cr_returned_date_sk", "int", str("0"), str("0"))
    draw_scatter(file_dir + file_name, 10, "cr_catalog_page_sk", "int", str("0"), str("0"))
    draw_scatter(file_dir + file_name, 10, "cr_returned_date_sk", "int", str("0"), str("0"))

    file_dir = "/mydata/tpcds_parquet_300.db_rewrite/catalog_sales/"
    files = os.listdir(file_dir)
    for file in files:
        if ".png" not in file:
            file_name = file
            break
    draw_scatter(file_dir + file_name, 10, "cs_sold_date_sk", "int", str("0"), str("0"))
    draw_scatter(file_dir + file_name, 10, "cs_order_number", "int", str("0"), str("0"))
    draw_scatter(file_dir + file_name, 10, "cs_ship_date_sk", "int", str("0"), str("0"))
    draw_scatter(file_dir + file_name, 10, "cs_catalog_page_sk", "int", str("0"), str("0"))

    file_dir = "/mydata/tpcds_parquet_300.db_rewrite/catalog_sales/"
    files = os.listdir(file_dir)
    for file in files:
        if ".png" not in file:
            file_name = file
            break
    draw_scatter(file_dir + file_name, 10, "cs_sold_date_sk", "int", str("0"), str("0"))
    draw_scatter(file_dir + file_name, 10, "cs_order_number", "int", str("0"), str("0"))
    draw_scatter(file_dir + file_name, 10, "cs_ship_date_sk", "int", str("0"), str("0"))
    draw_scatter(file_dir + file_name, 10, "cs_catalog_page_sk", "int", str("0"), str("0"))

    file_dir = "/mydata/tpcds_parquet_300.db_rewrite/customer/"
    files = os.listdir(file_dir)
    for file in files:
        if ".png" not in file:
            file_name = file
            break
    draw_scatter(file_dir + file_name, 10, "c_customer_sk", "int", str("0"), str("0"))

    file_dir = "/mydata/tpcds_parquet_300.db_rewrite/customer_address/"
    files = os.listdir(file_dir)
    for file in files:
        if ".png" not in file:
            file_name = file
            break
    draw_scatter(file_dir + file_name, 10, "ca_address_sk", "int", str("0"), str("0"))

    file_dir = "/mydata/tpcds_parquet_300.db_rewrite/customer_demographics/"
    files = os.listdir(file_dir)
    for file in files:
        if ".png" not in file:
            file_name = file
            break
    draw_scatter(file_dir + file_name, 10, "cd_demo_sk", "int", str("0"), str("0"))
    draw_scatter(file_dir + file_name, 10, "cd_dep_count", "int", str("0"), str("0"))

    file_dir = "/mydata/tpcds_parquet_300.db_rewrite/store_sales/"
    files = os.listdir(file_dir)
    for file in files:
        if ".png" not in file:
            file_name = file
            break
    draw_scatter(file_dir + file_name, 10, "ss_item_sk", "int", str("0"), str("0"))
    draw_scatter(file_dir + file_name, 10, "ss_customer_sk", "int", str("0"), str("0"))
    draw_scatter(file_dir + file_name, 10, "ss_addr_sk", "int", str("0"), str("0"))