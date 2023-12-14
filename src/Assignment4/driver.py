from pyspark.sql import SparkSession
from utils import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ReadJSON").getOrCreate()

    # Read JSON data with dynamic schema inference
    df = read_json_data(spark, r"C:\Users\Admin\OneDrive\Desktop\Pyspark Assignment\Output_files\Q4\Nested_json_file.json")
    df.show()

    # Perform various operations using functions from the utils file
    record_count = get_record_count(df)
    print("Record count:", record_count)

    df = flatten_json_data(df)
    df.show()

    df_with_schema = define_custom_schema(spark, r"C:\Users\Admin\OneDrive\Desktop\Pyspark Assignment\Output_files\Q4\Nested_json_file.json")
    df_with_date_columns = add_date_columns(df_with_schema)
    df_with_date_columns.show()

    write_partitioned_table(df_with_date_columns)
