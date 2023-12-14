import unittest
from pyspark.sql import SparkSession
from utils import *

class TestUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestReadJSON").getOrCreate()
        cls.file_path = "path_to_json_file"  # Replace "path_to_json_file" with your file path
        cls.df = read_json_data(cls.spark, cls.file_path)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_get_record_count(self):
        record_count = get_record_count(self.df)
        self.assertIsInstance(record_count, int)
        self.assertGreaterEqual(record_count, 0)

    def test_flatten_json_data(self):
        flattened_df = flatten_json_data(self.df)
        self.assertIsNotNone(flattened_df)
        self.assertEqual(flattened_df.columns, ["id", "properties", "employees"])

    def test_define_custom_schema(self):
        df_with_schema = define_custom_schema(self.spark, self.file_path)
        self.assertIsNotNone(df_with_schema)
        self.assertEqual(df_with_schema.columns, ["id", "properties", "employees"])

    def test_add_date_columns(self):
        df_with_date_columns = add_date_columns(self.df)
        self.assertIsNotNone(df_with_date_columns)
        self.assertEqual(df_with_date_columns.columns, ["id", "properties", "employees", "load_date", "year", "month", "day"])

    def test_write_partitioned_table(self):
        write_partitioned_table(self.df)
        # You can add assertions here to validate the writing process or check the table properties
        # For example, you can verify table existence or the number of partitions

if __name__ == '__main__':
    unittest.main()
