from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from PysparkRepo/src/Assignment1/util.py import *

# Sample data
purchase_data = [(1, "A"), (1, "B"), (2, "A"), (2, "B"), (3, "A"), (3, "B"),
                (1, "C"), (1, "D"), (1, "E"), (3, "E"), (4, "A")]

purchase_schema = StructType([
    StructField("customer", IntegerType(), True),
    StructField("product_model", StringType(), True)
])

product_schema = StructType([
    StructField("product_model", StringType(), True)
])

product_data = [("A",), ("B",), ("C",), ("D",), ("E",)]

# Creating DataFrames
purchase_data_df = spark.createDataFrame(purchase_data, ["customer", "product_model"])
product_data_df = spark.createDataFrame(product_data, ["product_model"])

# Display the DataFrames
purchase_data_df.show()
product_data_df.show()

# finding the customers who have bought only product A

filter_customers_bought_product_A(purchase_data_df).show()

# Finding the  customers who upgraded from product b to e

filter_customers_upgraded_B_to_E(purchase_data_df).show()

#Finding customers who have bought all models in the new Product Data

find_customers_bought_all_models(purchase_data_df, product_data_df).show()


