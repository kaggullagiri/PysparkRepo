from pyspark.sql import SparkSession
from PysparkRepo/src/Assignment3/util import *

def main():
    spark = SparkSession.builder.appName("UserActivityAnalysis").getOrCreate()
    return spark

# Create DataFrame
user_activity_df = create_dataframe(spark)

# Calculate number of actions performed by each user in the last 7 days
actions_last_7_days = actions_performed_last_7_days(user_activity_df)
actions_last_7_days.show()

# Convert timestamp column to login_date with yyyy-MM-dd format
user_activity_df = convert_timestamp_to_date(user_activity_df)
user_activity_df.show()

# Write DataFrame to CSV
csv_path = "c:/users/tables"
write_to_csv(user_activity_df, csv_path)

# Write DataFrame as managed table
write_as_managed_table(user_activity_df, "user", "login_details")

