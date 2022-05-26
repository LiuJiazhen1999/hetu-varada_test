import datetime
import os
import pyarrow.parquet as pp
import matplotlib.pyplot as plt
import numpy as np

from indexConstruct import get_sys_min_max_value, get_min_max_value



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
                if(_start == str(_)):
                    _flag = True
            else:
                if get_min_max_value(column_type, str(_), _start)[0] == _start and get_min_max_value(column_type, str(_), _end)[1] == _end:
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
		print(str(row_group_index) + "-" + str(_min) + "-" + str(_max))
    fig = plt.figure()
    ax1 = fig.add_subplot(1, 1, 1)
    ax1.set_title('Result Analysis')
    ax1.set_xlabel('min-value')
    ax1.set_ylabel('max-value')
    ax1.scatter(x1, y1, s=dot_size, c='lightgrey', marker='.')
    ax1.scatter(x2, y2, s=dot_size, c='k', marker='.')
    plt.savefig(file + ".png")
    plt.close()

if __name__ == "__main__":
    draw_scatter("/mydata/tpch_parquet_300.db_rewrite/customer/20220524_064704_00027_fs53m_1af48bb7-7f26-4364-8cca-751850202710",
                 10,
                 "acctbal",
                 "float",
                 "5000",
                 "5001")