from pyspark.sql.functions import *
from pyspark.sql.types import *

def define_employee_schema():
    # Define employee schema
    employee_schema = StructType([
        StructField("employee_id", IntegerType(), True),
        StructField("employee_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("State", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("Age", IntegerType(), True)
    ])
    return employee_schema

def define_department_schema():
    # Define department schema
    department_schema = StructType([
        StructField("dept_id", StringType(), True),
        StructField("dept_name", StringType(), True)
    ])
    return department_schema

def define_country_schema():
    # Define country schema
    country_schema = StructType([
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True)
    ])
    return country_schema

def create_dataframes(spark, employee_schema, department_schema, country_schema):
    # Data for creating DataFrames
    data_employee = [
        (11, "james", "D101", "ny", 9000, 34),
        (12, "michel", "D101", "ny", 8900, 32),
        (13, "robert", "D102", "ca", 7900, 29),
        (14, "scott", "D103", "ca", 8000, 36),
        (15, "jen", "D102", "ny", 9500, 38),
        (16, "jeff", "D103", "uk", 9100, 35),
        (17, "maria", "D101", "ny", 7900, 40)
    ]

    data_department = [
        ("D101", "sales"),
        ("D102", "finance"),
        ("D103", "marketing"),
        ("D104", "hr"),
        ("D105", "support")
    ]

    data_country = [
        ("ny", "newyork"),
        ("ca", "California"),
        ("uk", "Russia")
    ]

    # Create DataFrames using the defined schemas
    employee_df = spark.createDataFrame(data_employee, schema=employee_schema)
    department_df = spark.createDataFrame(data_department, schema=department_schema)
    country_df = spark.createDataFrame(data_country, schema=country_schema)

    return employee_df, department_df, country_df

def show_dataframes(employee_df, department_df, country_df):
    # Show the DataFrames
    employee_df.show()
    department_df.show()
    country_df.show()

def calculate_avg_salary(employee_df):
    # Q2: Find the average salary of each department
    return employee_df.groupBy("department").agg(avg("salary").alias("avg_salary"))

def find_employees_start_with_m(employee_df):
    # Q3: Find the employee name and department name whose name starts with 'm'
    return employee_df.filter(employee_df["employee_name"].startswith("m")).select("employee_name", "department")

def add_bonus_column(employee_df):
    # Q4: Create another new column 'bonus' by multiplying employee salary * 2
    return employee_df.withColumn("bonus", col("salary") * 2)

def reorder_columns(employee_df):
    # Q5: Reorder the column names of employee_df
    return employee_df.select("employee_id", "employee_name", "salary", "State", "Age", "department", "bonus")

def perform_joins(employee_df, department_df):
    # Q6: Perform Inner, Left, and Right joins dynamically
    inner_join = employee_df.join(department_df, employee_df.department == department_df.dept_id, "inner")
    left_join = employee_df.join(department_df, employee_df.department == department_df.dept_id, "left")
    right_join = employee_df.join(department_df, employee_df.department == department_df.dept_id, "right")

    return inner_join, left_join, right_join

def derive_new_dataframe(employee_df, country_df):
    # Q7: Derive a new DataFrame with 'country_name' instead of 'State' in employee_df
    new_employee_df = employee_df.join(country_df, employee_df.State == country_df.country_code, "left").drop("State").withColumnRenamed("country_name", "State")
    return new_employee_df

def convert_columns_and_add_load_date(new_employee_df):
    # Q8: Convert all column names into lowercase and add a 'load_date' column with the current date
    lowercase_columns = [col(column).alias(column.lower()) for column in new_employee_df.columns]
    return new_employee_df.select(lowercase_columns).withColumn("load_date", current_date())

def create_external_tables(final_df):
    # Q9: Create 2 external tables with parquet and csv formats
    final_df.write.mode("overwrite").parquet(r"C:\Users\Admin\OneDrive\Desktop\Pyspark Assignment\Output_files\Q5\parquet_file")
    final_df.write.mode("overwrite").csv(r"C:\Users\Admin\OneDrive\Desktop\Pyspark Assignment\Output_files\Q5\csv_file")
