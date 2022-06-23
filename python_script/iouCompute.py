import datetime
import os
import sys
from random import random

import pyarrow.parquet as pp

tpch_table_set = {
        "lineitem": [["orderkey", "partkey", "suppkey", "linenumber", "quantity", "extendedprice", "discount", "tax", "shipdate", "commitdate", "receiptdate"],
        ["int", "int", "int", "int", "float", "float", "float", "float", "date", "date", "date"]],
                      "part": [["partkey", "size", "retailprice"], ["int", "int", "float"]],
                      "supplier": [["suppkey", "nationkey", "acctbal"], ["int", "int", "float"]],
                      "partsupp": [["partkey", "suppkey", "availqty", "supplycost"], ["int", "int", "int", "float"]],
                      "customer": [["custkey", "nationkey", "acctbal"], ["int", "int", "float"]],
                      "orders": [["orderkey", "custkey", "totalprice", "orderdate", "shippriority"],
                                 ["int", "int", "float", "date", "int"]]
}

tpcds_table_set = {'call_center': [['cc_call_center_sk', 'cc_rec_start_date', 'cc_rec_end_date', 'cc_closed_date_sk', 'cc_open_date_sk', 'cc_employees', 'cc_sq_ft', 'cc_mkt_id', 'cc_division', 'cc_company', 'cc_gmt_offset', 'cc_tax_percentage'], ['int', 'date', 'date', 'int', 'int', 'int', 'int', 'int', 'int', 'int', 'float', 'float']],
                       'catalog_page': [['cp_catalog_page_sk', 'cp_start_date_sk', 'cp_end_date_sk', 'cp_catalog_number', 'cp_catalog_page_number'], ['int', 'int', 'int', 'int', 'int']],
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


def date2int(_date: str) -> int:
    _origin_date = datetime.date(1970, 1, 1)
    _cur_date = datetime.date(int(_date.split("-")[0]), int(_date.split("-")[1]), int(_date.split("-")[2]))
    interval = _cur_date - _origin_date
    return int(interval.days)

def compute_iou(_first, _second):
    if(max(_first[0], _second[0]) >= min(_first[1], _second[1])):
        return 0
    join_len = min(_first[1], _second[1]) - max(_first[0], _second[0])
    total_len = max(_first[1], _second[1]) - min(_first[0], _second[0])
    return join_len/total_len

def com_iou_random(file_path: str, column_name: str, column_type: str):
    _table = pp.ParquetFile(file_path)
    rg_num = _table.num_row_groups
    if rg_num < 20:
        return -1, 0, 0
    _sample_num = int(rg_num/5)
    if _sample_num % 2 == 1:
        _sample_num += 1
    sample_set = set()
    while len(sample_set) < _sample_num:
        sample_set.add(random.randint(0, rg_num - 1))
    print("iou_sample_set")
    print(sample_set)
    _cur_index = 0
    rgs1 = []
    rgs2 = []
    for _value in sample_set:
        if _cur_index % 2 == 0:
            rgs1.append(_value)
        else:
            rgs2.append(_value)
    assert len(rgs2) == len(rgs1)
    total_iou = 0
    total_count = 0
    _min = sys.maxsize
    _max = -sys.maxsize
    for _index in range(len(rgs1)):
        _first_min = sys.maxsize
        _first_max = -sys.maxsize
        _second_min = sys.maxsize
        _second_max = -sys.maxsize
        rg1_content = _table.read_row_group(rgs1[_index], columns=[column_name])
        for rg1_value in rg1_content.column(column_name):
            rg1_value = str(rg1_value)
            if rg1_value == "None":
                continue
            if column_type == "date":
                rg1_value = date2int(rg1_value)
            elif column_type == "int":
                rg1_value = int(rg1_value)
            elif column_type == "float":
                rg1_value = float(rg1_value)
            _first_max = max(_first_max, rg1_value)
            _first_min = min(_first_min, rg1_value)
        rg2_content = _table.read_row_group(rgs2[_index], columns=[column_name])
        for rg2_value in rg2_content.column(column_name):
            rg2_value = str(rg2_value)
            if rg2_value == "None":
                continue
            if column_type == "date":
                rg2_value = date2int(rg2_value)
            elif column_type == "int":
                rg2_value = int(rg2_value)
            elif column_type == "float":
                rg2_value = float(rg2_value)
            _second_max = max(_second_max, rg2_value)
            _second_min = min(_second_min, rg2_value)
        _min = min(_min, min(_first_min, _second_min))
        _max = max(_max, max(_first_max, _second_max))
        total_count += 1
        total_iou += compute_iou([_first_min, _first_max], [_second_min, _second_max])
    return total_iou / total_count, _min, _max

def com_iou(file_path: str, column_name: str, column_type: str):
    _table = pp.ParquetFile(file_path)
    rg_num = _table.num_row_groups
    if rg_num < 20:
        return -1, 0, 0
    total_iou = 0
    total_count = 0
    _min = sys.maxsize
    _max = -sys.maxsize
    for rg_index1 in range(0, int(rg_num/10), 2):
        _first_min = sys.maxsize
        _first_max = -sys.maxsize
        _second_min = sys.maxsize
        _second_max = -sys.maxsize
        rg1_content = _table.read_row_group(rg_index1, columns=[column_name])
        for rg1_value in rg1_content.column(column_name):
            rg1_value = str(rg1_value)
            if rg1_value == "None":
                continue
            if column_type == "date":
                rg1_value = date2int(rg1_value)
            elif column_type == "int":
                rg1_value = int(rg1_value)
            elif column_type == "float":
                rg1_value = float(rg1_value)
            _first_max = max(_first_max, rg1_value)
            _first_min = min(_first_min, rg1_value)
        rg2_content = _table.read_row_group(rg_index1 + 1, columns=[column_name])
        for rg2_value in rg2_content.column(column_name):
            rg2_value = str(rg2_value)
            if rg2_value == "None":
                continue
            if column_type == "date":
                rg2_value = date2int(rg2_value)
            elif column_type == "int":
                rg2_value = int(rg2_value)
            elif column_type == "float":
                rg2_value = float(rg2_value)
            _second_max = max(_second_max, rg2_value)
            _second_min = min(_second_min, rg2_value)
        _min = min(_min, min(_first_min, _second_min))
        _max = max(_max, max(_first_max, _second_max))
        total_count += 1
        total_iou += compute_iou([_first_min, _first_max], [_second_min, _second_max])
    return total_iou/total_count, _min, _max

if __name__ == "__main__":
    iou, _min, _max = com_iou("/mydata/tpch_parquet_300.db_rewrite/lineitem/20220524_040257_00019_fs53m_0ac223b1-073d-419a-a423-a83527104d5b", "extendedprice", "float")
    print(iou)
    print(_min)
    print(_max)
    # for tpch_table in tpch_table_set:
    #     tpch_dir = "/mydata/tpch_parquet_300.db_rewrite/"
    #     for i in range(len(tpch_table_set[tpch_table][0])):
    #         column_name = tpch_table_set[tpch_table][0][i]
    #         column_type = tpch_table_set[tpch_table][1][i]
    #         if column_type != "date" and column_type != "int" and column_type != "flaot":
    #             continue
    #         files = os.listdir(tpch_dir + tpch_table + "/")
    #         for file in files:
    #             if ".png" in file:
    #                 continue
    #             cur_iou, _min, _max = com_iou(tpch_dir + tpch_table + "/" + file, column_name, column_type)
    #             print("tpch-" + tpch_table + "-" + str(column_name) + "-" + str(column_type) + "-iou:" + str(cur_iou) + "-min:" + str(_min) + "-max:" + str(_max))
    #             break
    #
    # for tpcds_table in tpcds_table_set:
    #     tpcds_dir = "/mydata/tpcds_parquet_300.db_rewrite/"
    #     for i in range(len(tpcds_table_set[tpcds_table][0])):
    #         column_name = tpcds_table_set[tpcds_table][0][i]
    #         column_type = tpcds_table_set[tpcds_table][1][i]
    #         if column_type != "date" and column_type != "int" and column_type != "flaot":
    #             continue
    #         files = os.listdir(tpcds_dir + tpcds_table + "/")
    #         for file in files:
    #             if ".png" in file:
    #                 continue
    #             cur_iou, _min, _max = com_iou(tpcds_dir + tpcds_table + "/" + file, column_name, column_type)
    #             print("tpcds-" + tpcds_table + "-" + str(column_name) + "-" + str(column_type) + "-iou:" + str(cur_iou) + "-min:" + str(_min) + "-max:" + str(_max))
    #             break