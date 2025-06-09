from pyspark.sql.functions import dayofmonth, month, year
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, DateType
import datetime  # <-- needed for real date objects

spark = SparkSession.builder \
    .appName("Stock Analysis") \
    .getOrCreate()

# Convert strings to actual date objects
data_itc = [
    (datetime.date(2025, 5, 23), 18540000, 436.5),
    (datetime.date(2025, 5, 26), 13110000, 442.3),
    (datetime.date(2025, 5, 27), 26990000, 433.8),
    (datetime.date(2025, 5, 28), 431850000, 420.1),
    (datetime.date(2025, 5, 29), 21480000, 418.0),
]

data_infy = [
    (datetime.date(2025, 5, 23), 5040000, 1564.5),
    (datetime.date(2025, 5, 26), 3170000, 1580.3),
    (datetime.date(2025, 5, 27), 7780000, 1570.8),
    (datetime.date(2025, 5, 28), 3610000, 1571.1),
    (datetime.date(2025, 5, 29), 8610000, 1585.0),
]

# Define schemas
schema_stock1 = StructType([
    StructField("date", DateType(), True),
    StructField("volume_ITC", IntegerType(), True),
    StructField("close_ITC", FloatType(), True),
])

schema_stock2 = StructType([
    StructField("date", DateType(), True),
    StructField("volume_INFY", IntegerType(), True),
    StructField("close_INFY", FloatType(), True),
])

# Create DataFrames
df_itc = spark.createDataFrame(data_itc, schema=schema_stock1)
df_infy = spark.createDataFrame(data_infy, schema=schema_stock2)

# Add day, month, year columns
df_itc = df_itc.withColumn("day", dayofmonth("date")) \
               .withColumn("month", month("date")) \
               .withColumn("year", year("date"))

df_infy = df_infy.withColumn("day", dayofmonth("date")) \
                 .withColumn("month", month("date")) \
                 .withColumn("year", year("date"))

# Show results
print("ITC Data:")
df_itc.show()

print("Infosys Data:")
df_infy.show()
