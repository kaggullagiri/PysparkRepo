from pyspark.sql.functions import *

def filter_customers_bought_product_A(purchase_data_df):
    return purchase_data_df.filter(purchase_data_df["product_model"] == "A")

def filter_customers_upgraded_B_to_E(purchase_data_df):
    return purchase_data_df.filter(
        (purchase_data_df["product_model"] == "B") | (purchase_data_df["product_model"] == "E")
    ).filter(purchase_data_df["product_model"] == "E").select(purchase_data_df['customer'])

def find_customers_bought_all_models(purchase_data_df, product_data_df):
    distinct_models_count = product_data_df.distinct().count()
    return purchase_data_df.groupBy('customer') \
        .agg(collect_set('product_model').alias('collect_list')) \
        .filter(size('collect_list') == distinct_models_count) \
        .select(purchase_data_df['customer'])

