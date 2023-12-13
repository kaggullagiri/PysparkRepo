from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

def mask_card_number(card_num):
    return '*' * 12 + card_num[-4:]

def apply_mask_card_udf(credit_card_df):
    mask_card_udf = udf(mask_card_number, StringType())
    return credit_card_df.withColumn("masked_card_number", mask_card_udf(col("card_number")))
