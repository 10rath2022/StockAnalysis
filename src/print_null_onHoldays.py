from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when, lit

spark = SparkSession.builder.appName("INFY_NSE_Clean").getOrCreate()


# # Load CSV file
# Example schema: date, close_price
raw_nse = spark.read.csv("data/infy_nse_3yr.csv", header=True, inferSchema=True)
raw_nyse = spark.read.csv("data/infy_nyse_3yr.csv", header=True, inferSchema=True)

# Remove first two non-data rows
nse_df = raw_nse.filter((col("Price") != "Ticker") & (col("Price") != "Date"))
nyse_df = raw_nyse.filter((col("Price") != "Ticker") & (col("Price") != "Date"))



# Rename 'Price' to 'date_raw' and convert to proper date
nse_df = nse_df.withColumnRenamed("Price", "date_raw")
nse_df = nse_df.withColumn("date", to_date(col("date_raw"), "yyyy-MM-dd"))

nyse_df = nyse_df.withColumnRenamed("Price", "date_raw")



nyse_df = nyse_df.withColumn("date", to_date(col("date_raw"), "yyyy-MM-dd"))


# Drop the raw column if not needed
nse_df = nse_df.drop("date_raw")

nyse_df = nyse_df.drop("date_raw")

# # Show cleaned results
# nse_df.select("date", "Close", "Volume").show(10, truncate=False)


# nyse_df.select("date", "Close", "Volume").show(10, truncate=False)

## join using outer because we want to it all
combined_df = nse_df.alias("nse").join(
    nyse_df.alias("nyse"),
    on="date",
    how="outer"
)

final_df = combined_df.select(
    col("date"),
    when(col("nse.Close").isNull(), lit("HOLIDAY")).otherwise(col("nse.Close")).alias("nse_price"),
    when(col("nyse.Close").isNull(), lit("HOLIDAY")).otherwise(col("nyse.Close")).alias("nyse_price")
)

final_df.orderBy("date").show(truncate=False)

final_df.show(100)


# x date, y prices, 
# how to add two variable on one axis

import pandas as pd
import matplotlib.pyplot as plt

# Convert Spark DataFrame to Pandas
pandas_df = final_df.toPandas()

# Clean 'HOLIDAY' values and sort
pandas_df['nse_price'] = pd.to_numeric(pandas_df['nse_price'], errors='coerce')
pandas_df['nyse_price'] = pd.to_numeric(pandas_df['nyse_price'], errors='coerce')
pandas_df = pandas_df.sort_values("date")

# ------------------------------------------
# 📈 Plot 1: Both prices on one Y-axis (same scale)
# ------------------------------------------
plt.figure(figsize=(14, 6))
plt.plot(pandas_df["date"], pandas_df["nse_price"], label="NSE Price (INR)", color="blue")
plt.plot(pandas_df["date"], pandas_df["nyse_price"], label="NYSE Price (USD)", color="green")

plt.xlabel("Date")
plt.ylabel("Price (Mixed Scale)")
plt.title("INFY: NSE vs NYSE (Single Y-Axis)")
plt.legend(loc="lower right")  # Legend in bottom-right
plt.grid(True)
plt.tight_layout()
plt.show()


# ------------------------------------------
# 📈 Plot 2: Two Y-axes (useful for INR vs USD)
# 👉 Uncomment below to run dual Y-axis version
# ------------------------------------------

# fig, ax1 = plt.subplots(figsize=(14, 6))

# # NSE on left axis
# ax1.set_xlabel("Date")
# ax1.set_ylabel("NSE Price (INR)", color="blue")
# nse_line = ax1.plot(pandas_df["date"], pandas_df["nse_price"], color="blue", label="NSE (INR)")
# ax1.tick_params(axis='y', labelcolor="blue")

# # NYSE on right axis
# ax2 = ax1.twinx()
# ax2.set_ylabel("NYSE Price (USD)", color="green")
# nyse_line = ax2.plot(pandas_df["date"], pandas_df["nyse_price"], color="green", label="NYSE (USD)")
# ax2.tick_params(axis='y', labelcolor="green")

# # Combine legends from both axes
# lines = nse_line + nyse_line
# labels = [l.get_label() for l in lines]
# ax1.legend(lines, labels, loc="lower right")

# plt.title("INFY: NSE vs NYSE Prices (Dual Y-Axis)")
# fig.tight_layout()
# plt.grid(True)
# plt.show()
