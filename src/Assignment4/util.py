from pyspark.sql.functions import *
from pyspark.sql.types import *

def read_json_data(spark, file_path):
    df = spark.read.option("multiline", "true").json(file_path)
    return df 

def get_record_count(df):
    return df.count()

def flatten_json_data(df):
    flattened_df = df.withColumn("employees", explode(col("employees")))
    return flattened_df

def define_custom_schema(spark, file_path):
    custom_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("properties", StructType([
            StructField("name", StringType(), True),
            StructField("storeSize", StringType(), True)
        ]), True),
        StructField("employees", ArrayType(StructType([
            StructField("empId", IntegerType(), True),
            StructField("empName", StringType(), True)
        ])), True)
    ])
    df_with_schema = spark.read.schema(custom_schema).json(file_path)
    return df_with_schema

def add_date_columns(df):
    df_with_load_date = df.withColumn("load_date", current_date())
    df_with_date_columns = df_with_load_date.withColumn("year", year("load_date")).withColumn("month", month("load_date")).withColumn("day", dayofmonth("load_date"))
    return df_with_date_columns

def write_partitioned_table(df):
    df.write.partitionBy("year", "month", "day").mode("overwrite").format("json").saveAsTable("employee.employee_details")
