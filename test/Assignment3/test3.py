import unittest
from pyspark.sql import SparkSession
from

class TestUserActivity(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("Test").getOrCreate()
        self.user_activity_df = create_dataframe(self.spark)

    def tearDown(self):
        self.spark.stop()

    def test_actions_last_7_days(self):
        result_df = actions_performed_last_7_days(self.user_activity_df)
        expected_output = [(101, 3), (102, 3), (103, 2)]  # Replace with your expected output
        self.assertEqual(result_df.collect(), expected_output)

    def test_convert_timestamp_to_date(self):
        result_df = convert_timestamp_to_date(self.user_activity_df)
        self.assertIn("login_date", result_df.columns)
        self.assertEqual(result_df.count(), self.user_activity_df.count())

