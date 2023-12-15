
import unittest
from PysparkRepo/src/Assignment5/util import *
from pyspark.sql import SparkSession

class AssignmentTest(unittest.TestCase):
    def setUp(self): 
        self.spark = SparkSession.builder.appName("Test").getOrCreate()
        self.employee_df = create_employee_df(self.spark)
        self.department_df = create_department_df(self.spark)
        self.country_df = create_country_df(self.spark)

    def test_avg_salary_per_department(self):
        result = avg_salary_per_department(self.employee_df)
        self.assertEqual(result.count(), 4)  # Replace with the expected count

    def test_employee_name_department_starts_with_m(self):
        result = employee_name_department_starts_with_m(self.employee_df)
        self.assertEqual(result.count(), 2)  # Replace with the expected count

    def test_add_bonus_column(self):
        result = add_bonus_column(self.employee_df)
        self.assertEqual(result.columns,
                         ["employee_id", "employee_name", "department", "state", "salary", "age", "bonus"])

    def test_reorder_columns(self):
        result = reorder_columns(self.employee_df)
        self.assertEqual(result.columns, ["employee_id", "employee_name", "salary", "state", "age", "department"])

    def test_join_dataframes(self):
        result = join_dataframes(self.employee_df, self.department_df, "inner")
        self.assertEqual(result.count(), 7)  # Replace with the expected count

    def test_replace_state_with_country_name(self):
        result = replace_state_with_country_name(self.employee_df, self.country_df)
        self.assertEqual(result.columns,
                         ["employee_id", "employee_name", "department", "country_name", "salary", "age"])

    def test_convert_column_names_to_lower_case(self):
        result = convert_column_names_to_lower_case(self.employee_df)
        self.assertEqual(result.columns, ["employee_id", "employee_name", "department", "state", "salary", "age"])

    def test_add_load_date_column(self):
        result = add_load_date_column(self.employee_df)
        self.assertEqual(result.columns,["employee_id", "employee_name", "department", "state", "salary", "age", "load_date"])

