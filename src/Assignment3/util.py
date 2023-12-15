from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# defining the data
def create_dataframe(spark): 
    data = [
        (1, 101, 'login', '2023-09-05 08:30:00'),
        (2, 102, 'click', '2023-09-06 12:45:00'),
        (3, 101, 'click', '2023-09-07 14:15:00'),
        (4, 103, 'login', '2023-09-08 09:00:00'),
        (5, 102, 'logout', '2023-09-09 17:30:00'),
        (6, 101, 'click', '2023-09-10 11:20:00'),
        (7, 103, 'click', '2023-09-11 10:15:00'),
        (8, 102, 'click', '2023-09-12 13:10:00')
    ]
    # passing column names
    columns = ['log_id', 'user_id', 'user_activity', 'time_stamp']
    return spark.createDataFrame(data, columns)
    

def actions_performed_last_7_days(df):
    return df.groupBy('user_id').count()

def convert_timestamp_to_date(df):
    return df.withColumn('login_date', date_format(to_date('time_stamp'), 'yyyy-MM-dd'))

def write_to_csv(df, path):
    df.write.csv(path)

def write_as_managed_table(df, database_name, table_name):
    df.write.mode('overwrite').saveAsTable(f"{database_name}.{table_name}")
