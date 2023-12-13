import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, collect_set

from PysparkRepo/src/Assignment1/util import (
    filter_customers_bought_product_A,
    filter_customers_upgraded_B_to_E,
    find_customers_bought_all_models,
)

class TestPurchaseUtil(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Test").getOrCreate()
        purchase_data = [
            (1, "A"), (1, "B"), (2, "A"), (2, "B"), (3, "A"), (3, "B"),
            (1, "C"), (1, "D"), (1, "E"), (3, "E"), (4, "A")
        ]
        columns = ['customer', 'product_model']
        cls.purchase_data_df = cls.spark.createDataFrame(purchase_data, columns)
        product_data = [("A",), ("B",), ("C",), ("D",), ("E",)]
        cls.product_data_df = cls.spark.createDataFrame(product_data, ["product_model"])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_filter_customers_bought_product_A(self):
        result_df = filter_customers_bought_product_A(self.purchase_data_df)
        expected_output = self.purchase_data_df.filter(col("product_model") == "A")
        self.assertTrue(result_df.collect() == expected_output.collect())

    def test_filter_customers_upgraded_B_to_E(self):
        result_df = filter_customers_upgraded_B_to_E(self.purchase_data_df)
        expected_output = self.purchase_data_df.filter(
            (col("product_model").isin(["B", "E"])) & (col("product_model") == "E")
        ).select("customer")
        self.assertTrue(result_df.collect() == expected_output.collect())

    def test_find_customers_bought_all_models(self):
        result_df = find_customers_bought_all_models(self.purchase_data_df, self.product_data_df)
        distinct_models_count = self.product_data_df.distinct().count()
        expected_output = self.purchase_data_df.groupBy('customer') \
            .agg(collect_set('product_model').alias('collect_list')) \
            .filter(size('collect_list') == distinct_models_count) \
            .select('customer')
        self.assertTrue(result_df.collect() == expected_output.collect())


