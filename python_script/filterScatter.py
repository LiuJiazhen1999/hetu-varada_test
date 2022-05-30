import datetime
import os
import pyarrow.parquet as pp
import matplotlib.pyplot as plt
import numpy as np

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
    ax1.set_title('Result Analysis')
    ax1.set_xlabel('min-value')
    ax1.set_ylabel('max-value')
    ax1.scatter(x1, y1, s=dot_size, c='lightgrey', marker='.')
    ax1.scatter(x2, y2, s=dot_size, c='k', marker='.')
    plt.savefig(file + column + "_" + _start + "-" + _end + ".png")
    plt.close()

if __name__ == "__main__":
    # for i in range(1, 7):
    #         draw_scatter(
    #         "/mydata/tpch_parquet_300.db_rewrite/orders/20220524_062839_00026_fs53m_66cba92c-a213-4e48-ab15-5b37a5bb0daa",
    #         10,
    #         "orderdate",
    #         "date",
    #         "199" + str(i) + "-3-1",
    #         "199" + str(i) + "-3-1")

    for i in range(0, 7):
            draw_scatter("/mydata/tpcds_parquet_300.db_rewrite/catalog_returns/20220523_132505_00004_fs53m_509e9f99-1d58-4cf7-9d90-98845500e399",
                 10,
                 "cr_returned_date_sk",
                 "float",
                 str(2450750 + i * 100),
                 str(2450750 + (i + 1) * 100))

    for i in range(0, 7):
            draw_scatter("/mydata/tpcds_parquet_300.db_rewrite/catalog_returns/20220523_132505_00004_fs53m_509e9f99-1d58-4cf7-9d90-98845500e399",
                 10,
                 "cr_returned_date_sk",
                 "float",
                 str(2450750 + i * 250),
                 str(2450750 + i * 250))

    for i in range(0, 7):
            draw_scatter("/mydata/tpcds_parquet_300.db_rewrite/catalog_returns/20220523_132505_00004_fs53m_509e9f99-1d58-4cf7-9d90-98845500e399",
                 10,
                 "cr_returned_time_sk",
                 "float",
                 str(i * 10000),
                 str(i * 10000 + 1000))
    for i in range(0, 7):
            draw_scatter("/mydata/tpcds_parquet_300.db_rewrite/catalog_returns/20220523_132505_00004_fs53m_509e9f99-1d58-4cf7-9d90-98845500e399",
                 10,
                 "cr_returned_time_sk",
                 "float",
                 str(i * 10000 + 2000),
                 str(i * 10000 + 2000))
    for i in range(0, 7):
            draw_scatter("/mydata/tpcds_parquet_300.db_rewrite/catalog_returns/20220523_132505_00004_fs53m_509e9f99-1d58-4cf7-9d90-98845500e399",
                 10,
                 "cr_item_sk",
                 "float",
                 str(i * 30000),
                 str(i * 30000 + 5000))
    for i in range(0, 7):
            draw_scatter("/mydata/tpcds_parquet_300.db_rewrite/catalog_returns/20220523_132505_00004_fs53m_509e9f99-1d58-4cf7-9d90-98845500e399",
                 10,
                 "cr_item_sk",
                 "float",
                 str(i * 30000 + 3000),
                 str(i * 30000 + 3000))
    for i in range(0, 7):
            draw_scatter("/mydata/tpcds_parquet_300.db_rewrite/catalog_returns/20220523_132505_00004_fs53m_509e9f99-1d58-4cf7-9d90-98845500e399",
                 10,
                 "cr_order_number",
                 "float",
                 str(i * 5000000),
                 str(i * 5000000 + 10000))
    for i in range(0, 7):
            draw_scatter("/mydata/tpcds_parquet_300.db_rewrite/catalog_returns/20220523_132505_00004_fs53m_509e9f99-1d58-4cf7-9d90-98845500e399",
                 10,
                 "cr_order_number",
                 "float",
                 str(i * 5000000 + 3000),
                 str(i * 5000000 + 3000))

    # for i in range(0, 7):
    #         draw_scatter("/mydata/tpcds_parquet_300.db_rewrite/catalog_sales/20220523_132919_00005_fs53m_ff916c89-b7ca-4b23-b0f1-efd34a8ec887",
    #              10,
    #              "cs_order_number",
    #              "float",
    #              str(i * 5000000),
    #              str(i * 5000000 + 10000))
    # for i in range(0, 7):
    #         draw_scatter("/mydata/tpcds_parquet_300.db_rewrite/catalog_sales/20220523_132919_00005_fs53m_ff916c89-b7ca-4b23-b0f1-efd34a8ec887",
    #              10,
    #              "cs_order_number",
    #              "float",
    #              str(i * 5000000 + 3000),
    #              str(i * 5000000 + 3000))
    # for i in range(-2, 4):
    #         draw_scatter("/mydata/tpcds_parquet_300.db_rewrite/catalog_sales/20220523_132919_00005_fs53m_ff916c89-b7ca-4b23-b0f1-efd34a8ec887",
    #              10,
    #              "cs_net_profit",
    #              "float",
    #              str(i * 5000),
    #              str(i * 5000 + 500))
    # for i in range(-2, 4):
    #         draw_scatter("/mydata/tpcds_parquet_300.db_rewrite/catalog_sales/20220523_132919_00005_fs53m_ff916c89-b7ca-4b23-b0f1-efd34a8ec887",
    #              10,
    #              "cs_net_profit",
    #              "float",
    #              str(i * 5000 + 300),
    #              str(i * 5000 + 300))
    # for i in range(0, 8):
    #         draw_scatter("/mydata/tpcds_parquet_300.db_rewrite/catalog_sales/20220523_132919_00005_fs53m_ff916c89-b7ca-4b23-b0f1-efd34a8ec887",
    #              10,
    #              "cs_net_paid_inc_ship_tax",
    #              "float",
    #              str(i * 5000),
    #              str(i * 5000 + 500))
    # for i in range(0, 8):
    #         draw_scatter("/mydata/tpcds_parquet_300.db_rewrite/catalog_sales/20220523_132919_00005_fs53m_ff916c89-b7ca-4b23-b0f1-efd34a8ec887",
    #              10,
    #              "cs_net_paid_inc_ship_tax",
    #              "float",
    #              str(i * 5000 + 300),
    #              str(i * 5000 + 300))
    # for i in range(0, 7):
    #         draw_scatter("/mydata/tpcds_parquet_300.db_rewrite/catalog_sales/20220523_132919_00005_fs53m_ff916c89-b7ca-4b23-b0f1-efd34a8ec887",
    #              10,
    #              "cs_net_paid_inc_ship",
    #              "float",
    #              str(i * 5000),
    #              str(i * 5000 + 500))
    # for i in range(0, 7):
    #         draw_scatter("/mydata/tpcds_parquet_300.db_rewrite/catalog_sales/20220523_132919_00005_fs53m_ff916c89-b7ca-4b23-b0f1-efd34a8ec887",
    #              10,
    #              "cs_net_paid_inc_ship",
    #              "float",
    #              str(i * 5000 + 300),
    #              str(i * 5000 + 300))
    #
    # for i in range(0, 7):
    #         draw_scatter("/mydata/tpcds_parquet_300.db_rewrite/customer_address/20220523_141210_00007_fs53m_f74350a2-39bd-4d7c-966f-2d691ea63d0a",
    #              10,
    #              "ca_address_sk",
    #              "float",
    #              str(i * 300000),
    #              str(i * 300000 + 30000))
    # for i in range(0, 7):
    #         draw_scatter("/mydata/tpcds_parquet_300.db_rewrite/customer_address/20220523_141210_00007_fs53m_f74350a2-39bd-4d7c-966f-2d691ea63d0a",
    #              10,
    #              "ca_address_sk",
    #              "float",
    #              str(i * 300000 + 300),
    #              str(i * 300000 + 300))