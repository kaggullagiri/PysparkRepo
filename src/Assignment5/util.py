
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


def create_employee_df(spark):
    employee_schema = StructType([
        StructField("employee_id", IntegerType(), True),
        StructField("employee_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("state", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("age", IntegerType(), True)
    ])

    employee_data = [
        (11, "james", "D101", "ny", 9000, 34),
        (12, "michel", "D101", "ny", 8900, 32),
        (13, "robert", "D102", "ca", 7900, 29),
        (14, "scott", "D103", "ca", 8000, 36),
        (15, "jen", "D102", "ny", 9500, 38),
        (16, "jeff", "D103", "uk", 9100, 35),
        (17, "maria", "D101", "ny", 7900, 40)
    ]

    return spark.createDataFrame(employee_data, schema=employee_schema)


def create_department_df(spark):
    department_schema = StructType([
        StructField("dept_id", StringType(), True),
        StructField("dept_name", StringType(), True)
    ])

    department_data = [
        ("D101", "sales"),
        ("D102", "finance"),
        ("D103", "marketing"),
        ("D104", "hr"),
        ("D105", "support")
    ]

    return spark.createDataFrame(department_data, schema=department_schema)


def create_country_df(spark):
    country_schema = StructType([
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True)
    ])
    country_data = [
        ("ny", "newyork"),
        ("ca", "California"),
        ("uk", "Russia")]

    return spark.createDataFrame(country_data, schema=country_schema)


def avg_salary_per_department(employee_df):
    return employee_df.groupBy("department").agg(expr("avg(salary) as avg_salary"))


def employee_name_department_starts_with_m(employee_df):
    return employee_df.filter(col("employee_name").startswith("m")).select("employee_name", "department")


def add_bonus_column(employee_df):
    return employee_df.withColumn("bonus", col("salary") * 2)


def reorder_columns(employee_df):
    return employee_df.select("employee_id", "employee_name", "salary", "state", "age", "department")


def join_dataframes(employee_df, department_df, join_type):
    return employee_df.join(department_df, employee_df.department == department_df.dept_id, how=join_type)


def replace_state_with_country_name(employee_df, country_df):
    return employee_df.join(country_df, employee_df.state == country_df.country_code, "left").drop(
        "state").withColumnRenamed("country_name", "state")


def convert_column_names_to_lower_case(employee_df):
    return employee_df.toDF(*[col.lower() for col in employee_df.columns])


def add_load_date_column(employee_df):
    return employee_df.withColumn("load_date", current_date())
