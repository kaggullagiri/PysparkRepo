from pyspark.sql import SparkSession
from PysparkRepo/src/Assignment2/util import *

# Sample data
data = [
        ("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)
]

# Create DataFrame
credit_card_df = spark.createDataFrame(data, ["card_number"])

credit_card_df = spark.read.csv('dbfs:/path/src/file.csv', header=False, inferSchema=True)
credit_card_df.show()

# Print number of partitions
print("No of partitions before adjustment:", credit_card_df.rdd.getNumPartitions())

# Increase the partition size to 5
credit_card_df = credit_card_df.repartition(5)

# Decrease the partition size back to its original partition size
credit_card_df = credit_card_df.coalesce(1)

# Save DataFrame to disk
credit_card_df.write.mode('overwrite').parquet("/tmp/credit_card_data")

# Read DataFrame back into memory
credit_card_df = spark.read.parquet("/tmp/credit_card_data")

# Apply UDF to mask card numbers
credit_card_df = apply_mask_card_udf(credit_card_df)

# Show DataFrame
credit_card_df.show()

