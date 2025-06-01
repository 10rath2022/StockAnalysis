from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
import time
import pandas as pd
import matplotlib.pyplot as plt
import os, sys


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


start_time = time.time()

spark = SparkSession.builder.appName("StockMergeOptimized").getOrCreate()

# Load CSVs with no header
itc_raw = spark.read.csv("./data/itc_nse_3yr.csv", header=True)
infy_raw = spark.read.csv("./data/infy_nse_3yr.csv", header=True, inferSchema=True)
infy_raw.printSchema()


sys.exit(0)
# infy_raw = infy_raw.rdd.zipWithIndex() \
#     .filter(lambda x: x[1] >= 2) \
#     .map(lambda x: x[0]) \
#     .toDF()
print(f"Type of infy_raw{type(infy_raw)}")
# infy_raw.show()

window_spec = Window.orderBy("date")  # You can order by any column(s)

df_with_row_num = infy_raw.withColumn("row_num", row_number().over(window_spec))

i = 2
j = 14
filtered_df = df_with_row_num.filter((df_with_row_num.row_num >= i) & (df_with_row_num.row_num <= j))


filtered_df.limit(10).show()




sys.exit(0)

# Filter and select columns early
itc_clean = itc_raw.filter(col("_c0").rlike("^20")) \
                   .select(
                      col("_c0").alias("date"),
                      col("_c1").cast("double").alias("close_stock1"),
                      col("_c5").cast("long").alias("volume_stock1")
                   ).cache()

infy_clean = infy_raw.filter(col("_c0").rlike("^20")) \
                     .select(
                        col("_c0").alias("date"),
                        col("_c1").cast("double").alias("close_stock2"),
                        col("_c5").cast("long").alias("volume_stock2")
                     ).cache()



# Broadcast join if one dataset is smaller (try broadcasting infy)
joined_df = itc_clean.join(broadcast(infy_clean), on="date", how="inner")

sys.exit()
# Collect for plotting
pandas_df = joined_df.orderBy("date").toPandas()
pandas_df['date'] = pd.to_datetime(pandas_df['date'].astype(str))

print(f"Join and load execution time: {time.time() - start_time:.2f} seconds")

# Plot volume
plt.figure(figsize=(12, 6))
plt.plot(pandas_df['date'], pandas_df['volume_stock1'], label='Volume Stock 1')
plt.plot(pandas_df['date'], pandas_df['volume_stock2'], label='Volume Stock 2')
plt.xlabel('Date')
plt.ylabel('Volume')
plt.title('Volume of Stocks Over Time')
plt.legend()
plt.show()

# Plot closing prices
plt.figure(figsize=(12, 6))
plt.plot(pandas_df['date'], pandas_df['close_stock1'], label='Close Stock 1')
plt.plot(pandas_df['date'], pandas_df['close_stock2'], label='Close Stock 2')
plt.xlabel('Date')
plt.ylabel('Closing Price')
plt.title('Closing Price of Stocks Over Time')
plt.legend()
plt.show()
