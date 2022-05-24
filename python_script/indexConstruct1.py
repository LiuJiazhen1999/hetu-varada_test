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


def tpch_construct():
    file_dir = "/mydata/tpch_parquet_300.db_rewrite"
    index_dir = file_dir + "_index/"
    file_dir += "/"
    _tables = os.listdir(file_dir)
    tpch_table_set = {"lineitem": [
        ["orderkey", "partkey", "suppkey", "linenumber", "quantity", "extendedprice", "discount", "tax", "shipdate",
         "commitdate", "receiptdate"],
        ["int", "int", "int", "int", "float", "float", "float", "float", "date", "date", "date"]],
                      "part": [["partkey", "size", "retailprice"], ["int", "int", "float"]],
                      "supplier": [["suppkey", "nationkey", "acctbal"], ["int", "int", "float"]],
                      "partsupp": [["partkey", "suppkey", "availqty", "supplycost"], ["int", "int", "int", "float"]],
                      "customer": [["custkey", "nationkey", "acctbal"], ["int", "int", "float"]],
                      "orders": [["orderkey", "custkey", "totalprice", "orderdate", "shippriority"],
                                 ["int", "int", "float", "date", "int"]]}
    for _table in _tables:
        if not os.path.exists(index_dir + _table):
            os.makedirs(index_dir + _table)
        _files = os.listdir(file_dir + _table)
        _files = [file_dir + _table + "/" + file for file in _files]
        constructORCLikeIndex(index_dir + _table + "/", _files, tpch_table_set[str(_table)][0], tpch_table_set[str(_table)][1])

def tpcds_construct():
    file_dir = "/mydata/tpcds_parquet_300.db_rewrite"
    index_dir = file_dir + "_index/"
    file_dir += "/"
    _tables = os.listdir(file_dir)
    tpcds_table_set = {'call_center': [['cc_call_center_sk', 'cc_rec_start_date', 'cc_rec_end_date', 'cc_closed_date_sk', 'cc_open_date_sk', 'cc_employees', 'cc_sq_ft', 'cc_mkt_id', 'cc_division', 'cc_company', 'cc_gmt_offset', 'cc_tax_percentage'], ['int', 'date', 'date', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'float', 'float']], 'cp': [['cp_catalog_page_sk', 'cp_start_date_sk', 'cp_end_date_sk', 'cp_catalog_number', 'cp_catalog_page_number'], ['int', 'int', 'int', 'int', 'int']],
                       'catalog_returns': [['cr_returned_date_sk', 'cr_returned_time_sk', 'cr_item_sk', 'cr_refunded_customer_sk', 'cr_refunded_cdemo_sk', 'cr_refunded_hdemo_sk', 'cr_refunded_addr_sk', 'cr_returning_customer_sk', 'cr_returning_cdemo_sk', 'cr_returning_hdemo_sk', 'cr_returning_addr_sk', 'cr_call_center_sk', 'cr_catalog_page_sk', 'cr_ship_mode_sk', 'cr_warehouse_sk', 'cr_reason_sk', 'cr_order_number', 'cr_return_quantity', 'cr_return_amount', 'cr_return_tax', 'cr_return_amt_inc_tax', 'cr_fee', 'cr_return_ship_cost', 'cr_refunded_cash', 'cr_store_credit', 'cr_net_loss'], ['int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float']],
                       'catalog_sales': [['cs_sold_date_sk', 'cs_sold_time_sk', 'cs_ship_date_sk', 'cs_bill_customer_sk', 'cs_bill_cdemo_sk', 'cs_bill_hdemo_sk', 'cs_bill_addr_sk', 'cs_ship_customer_sk', 'cs_ship_cdemo_sk', 'cs_ship_hdemo_sk', 'cs_ship_addr_sk', 'cs_call_center_sk', 'cs_catalog_page_sk', 'cs_ship_mode_sk', 'cs_warehouse_sk', 'cs_item_sk', 'cs_promo_sk', 'cs_order_number', 'cs_quantity', 'cs_wholesale_cost', 'cs_list_price', 'cs_sales_price', 'cs_ext_discount_amt', 'cs_ext_sales_price', 'cs_ext_wholesale_cost', 'cs_ext_list_price', 'cs_ext_tax', 'cs_coupon_amt', 'cs_ext_ship_cost', 'cs_net_paid', 'cs_net_paid_inc_tax', 'cs_net_paid_inc_ship', 'cs_net_paid_inc_ship_tax', 'cs_net_profit'], ['int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float']],
                       'customer': [['c_customer_sk', 'c_current_cdemo_sk', 'c_current_hdemo_sk', 'c_current_addr_sk', 'c_first_shipto_date_sk', 'c_first_sales_date_sk', 'c_birth_day', 'c_birth_month', 'c_birth_year', 'c_last_review_date_sk'], ['int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int']],
                       'customer_address': [['ca_address_sk', 'ca_gmt_offset'], ['int', 'float']],
                       'customer_demographics': [['cd_demo_sk', 'cd_purchase_estimate', 'cd_dep_count', 'cd_dep_employed_count', 'cd_dep_college_count'], ['int', 'int', 'int', 'int', 'int']],
                       'date_dim': [['d_date_sk', 'd_date', 'd_month_seq', 'd_week_seq', 'd_quarter_seq', 'd_year', 'd_dow', 'd_moy', 'd_dom', 'd_qoy', 'd_fy_year', 'd_fy_quarter_seq', 'd_fy_week_seq', 'd_first_dom', 'd_last_dom', 'd_same_day_ly', 'd_same_day_lq'], ['int', 'date', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int']],
                       'household_demographics': [['hd_demo_sk', 'hd_income_band_sk', 'hd_dep_count', 'hd_vehicle_count'], ['int', 'int', 'int', 'int']],
                       'income_band': [['ib_income_band_sk', 'ib_lower_bound', 'ib_upper_bound'], ['int', 'int', 'int']],
                       'inventory': [['inv_date_sk', 'inv_item_sk', 'inv_warehouse_sk', 'inv_quantity_on_hand'], ['int', 'int', 'int', 'int']],
                       'item': [['i_item_sk', 'i_rec_start_date', 'i_rec_end_date', 'i_current_price', 'i_wholesale_cost', 'i_brand_id', 'i_class_id', 'i_category_id', 'i_manufact_id', 'i_manager_id'], ['int', 'date', 'date', 'float', 'float', 'int', 'int', 'int', 'int', 'int']],
                       'promotion': [['p_promo_sk', 'p_start_date_sk', 'p_end_date_sk', 'p_item_sk', 'p_cost', 'p_response_targe'], ['int', 'int', 'int', 'int', 'float', 'int']],
                       'reason': [['r_reason_sk'], ['int']],
                       'ship_mode': [['sm_ship_mode_sk'], ['int']],
                       'store': [['s_store_sk', 's_rec_start_date', 's_rec_end_date', 's_closed_date_sk', 's_number_employees', 's_floor_space', 's_market_id', 's_division_id', 's_company_id', 's_gmt_offset', 's_tax_precentage'], ['int', 'date', 'date', 'int', 'int', 'int', 'int', 'int', 'int', 'float', 'float']],
                       'store_returns': [['sr_returned_date_sk', 'sr_return_time_sk', 'sr_item_sk', 'sr_customer_sk', 'sr_cdemo_sk', 'sr_hdemo_sk', 'sr_addr_sk', 'sr_store_sk', 'sr_reason_sk', 'sr_ticket_number', 'sr_return_quantity', 'sr_return_amt', 'sr_return_tax', 'sr_return_amt_inc_tax', 'sr_fee', 'sr_return_ship_cost', 'sr_refunded_cash', 'sr_store_credit', 'sr_net_loss'], ['int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float']],
                       'store_sales': [['ss_sold_date_sk', 'ss_sold_time_sk', 'ss_item_sk', 'ss_customer_sk', 'ss_cdemo_sk', 'ss_hdemo_sk', 'ss_addr_sk', 'ss_store_sk', 'ss_promo_sk', 'ss_ticket_number', 'ss_quantity', 'ss_wholesale_cost', 'ss_list_price', 'ss_sales_price', 'ss_ext_discount_amt', 'ss_ext_sales_price', 'ss_ext_wholesale_cost', 'ss_ext_list_price', 'ss_ext_tax', 'ss_coupon_amt', 'ss_net_paid', 'ss_net_paid_inc_tax', 'ss_net_profit'], ['int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float']],
                       'time_dim': [['t_time_sk', 't_time', 't_hour', 't_minute', 't_second'], ['int', 'int', 'int', 'int', 'int']],
                       'warehouse': [['w_warehouse_sk', 'w_warehouse_sq_ft', 'w_gmt_offset'], ['int', 'int', 'float']],
                       'web_page': [['wp_web_page_sk', 'wp_rec_start_date', 'wp_rec_end_date', 'wp_creation_date_sk', 'wp_access_date_sk', 'wp_customer_sk', 'wp_link_count', 'wp_image_count', 'wp_max_ad_count'], ['int', 'date', 'date', 'int', 'int', 'int', 'int', 'int', 'int']],
                       'web_returns': [['wr_returned_date_sk', 'wr_returned_time_sk', 'wr_item_sk', 'wr_refunded_customer_sk', 'wr_refunded_cdemo_sk', 'wr_refunded_hdemo_sk', 'wr_refunded_addr_sk', 'wr_returning_customer_sk', 'wr_returning_cdemo_sk', 'wr_returning_hdemo_sk', 'wr_returning_addr_sk', 'wr_web_page_sk', 'wr_reason_sk', 'wr_order_number', 'wr_return_quantity', 'wr_return_amt', 'wr_return_tax', 'wr_return_amt_inc_tax', 'wr_fee', 'wr_return_ship_cost', 'wr_refunded_cash', 'wr_account_credit', 'wr_net_loss'], ['int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float']],
                       'web_sales': [['ws_sold_date_sk', 'ws_sold_time_sk', 'ws_ship_date_sk', 'ws_item_sk', 'ws_bill_customer_sk', 'ws_bill_cdemo_sk', 'ws_bill_hdemo_sk', 'ws_bill_addr_sk', 'ws_ship_customer_sk', 'ws_ship_cdemo_sk', 'ws_ship_hdemo_sk', 'ws_ship_addr_sk', 'ws_web_page_sk', 'ws_web_site_sk', 'ws_ship_mode_sk', 'ws_warehouse_sk', 'ws_promo_sk', 'ws_order_number', 'ws_quantity', 'ws_wholesale_cost', 'ws_list_price', 'ws_sales_price', 'ws_ext_discount_amt', 'ws_ext_sales_price', 'ws_ext_wholesale_cost', 'ws_ext_list_price', 'ws_ext_tax', 'ws_coupon_amt', 'ws_ext_ship_cost', 'ws_net_paid', 'ws_net_paid_inc_tax', 'ws_net_paid_inc_ship', 'ws_net_paid_inc_ship_tax', 'ws_net_profit'], ['int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float', 'float']],
                       'web_site': [['web_site_sk', 'web_rec_start_date', 'web_rec_end_date', 'web_open_date_sk', 'web_close_date_sk', 'web_mkt_id', 'web_company_id', 'web_gmt_offset', 'web_tax_percentage'], ['int', 'date', 'date', 'int', 'int', 'int', 'int', 'float', 'float']]}

    for _table in _tables:
        if not os.path.exists(index_dir + _table):
            os.makedirs(index_dir + _table)
        _files = os.listdir(file_dir + _table)
        _files = [file_dir + _table + "/" + file for file in _files]
        constructORCLikeIndex(index_dir + _table + "/", _files, tpcds_table_set[str(_table)][0], tpcds_table_set[str(_table)][1])

if __name__ == "__main__":
    tpcds_construct()