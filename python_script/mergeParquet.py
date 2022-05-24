#注意：排序需要python3.8 pyarrow的版本为8.0.0
#sudo apt install python3.8
# sudo rm /usr/bin/python3
# sudo ln -s /usr/bin/python3.8 /usr/bin/python3
# pip3 install --upgrade pip
# pip3 install pyarrow==8.0.0
import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

#
# Warning!!!
# Suffers from the same problem as the parquet-tools merge function
#
#parquet-tools merge:
#Merges multiple Parquet files into one. The command doesn't merge row groups,
#just places one after the other. When used to merge many small files, the
#resulting file will still contain small row groups, which usually leads to bad
#query performance.

def arrow_sort_values(table: pa.lib.Table, by: str or list) -> pa.lib.Table:
    """
    Sort an Arrow table. Same as sort_values for a Dataframe.
    :param table: Arrow table.
    :param by: Column names to sort by. String or array.
    :return: Sorted Arrow table.
    """
    table_sorted_indexes = pa.compute.bottom_k_unstable(table, sort_keys=by, k=len(table))
    table_sorted = table.take(table_sorted_indexes)
    return table_sorted

def combine_parquet_files(input_folder, target_path):
    try:
        files = []
        for file_name in os.listdir(input_folder):
            files.append(pq.read_table(os.path.join(input_folder, file_name)))
        with pq.ParquetWriter(target_path,
                            files[0].schema,
                            version='2.4',
                            compression='gzip',
                            use_dictionary=True,
                            data_page_size=2097152, #2MB
                            write_statistics=True) as writer:
            for f in files:
                writer.write_table(f)
    except Exception as e:
        print(e)

if __name__=="__main__":
    origin_file_dir = "/mydata/tpch_parquet_300.db"
    rewrite_dir = origin_file_dir + "_rewrite/"
    origin_file_dir += "/"
    origin_tables = os.listdir(origin_file_dir)
    for origin_table in origin_tables:
        if not os.path.exists(rewrite_dir + origin_table)
            os.makedirs(rewrite_dir + origin_table)
        _files = os.listdir(origin_file_dir + origin_table)
        for _file in _files:
            _table = pq.read_table(origin_file_dir + origin_table + "/" + _file)
            # _table = arrow_sort_values(_table, by=["shipdate"])
            pq.write_table(_table, rewrite_dir + origin_table + "/" + _file, row_group_size=1000)