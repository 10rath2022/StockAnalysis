# # import timeit

# # # Code as string
# # timeit.timeit("x = sum(range(100))", number=1000)

# # # With a function
# # def test():
# #     return sum(range(100))

# # timeit.timeit(test, number=1000)

# import timeit

# print("List comprehension:", timeit.timeit("x = [i for i in range(1000)]", number=10000))
# print("list(range()):", timeit.timeit("x = list(range(1000))", number=10000))

# # Measure function performance

# def square_loop():
#     return [i*i for i in range(1000)]

# print("Measure function performance:",timeit.timeit(square_loop, number=10000))


# # Time sorting a list

# setup_code = "import random; x = random.sample(range(10000), 1000)"
# test_code = "sorted(x)"
# print("sorting a list:",timeit.timeit(stmt=test_code, setup=setup_code, number=1000))

# import timeit

# def add_loop():
#     return sum(range(10))

# print("10 times:", timeit.timeit(add_loop, number=10))      # Less accurate
# print("10,000 times:", timeit.timeit(add_loop, number=10000))  # More stable


from pyspark.sql import SparkSession
import timeit

# Start Spark session
spark = SparkSession.builder \
    .appName("JoinTimeExample") \
    .getOrCreate()

# Create dummy DataFrames
df1 = spark.createDataFrame(
    [(1, "Alice"), (2, "Bob"), (3, "Cathy"), (4, "David")],
    ["id", "name"]
)

df2 = spark.createDataFrame(
    [(1, "Math"), (2, "Science"), (3, "History"), (5, "Art")],
    ["id", "subject"]
)

# Cache data for fair comparison (avoid recomputation)
df1.cache()
df2.cache()

# Function to perform inner join
def inner_join():
    df1.join(df2, on="id", how="inner").collect()

# Function to perform left join
def left_join():
    df1.join(df2, on="id", how="left").collect()

# Function to perform right join
def right_join():
    df1.join(df2, on="id", how="right").collect()

# Time the joins
print("Inner Join Time:", timeit.timeit(inner_join, number=10))
print("Left Join Time :", timeit.timeit(left_join, number=10))
print("Right Join Time:", timeit.timeit(right_join, number=10))

# Stop Spark
spark.stop()
