from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Sample PySpark Script") \
    .getOrCreate()

# Read a CSV file into a DataFrame
df = spark.read.csv("input.csv", header=True, inferSchema=True)

# Perform some transformations
# Example: Group by a column and compute sum of another column
result_df = df.groupBy("category").agg(sum(col("amount")).alias("total_amount"))

# Show the result
result_df.show()

# Write the result to a CSV file
result_df.write.csv("output.csv", header=True)

# Stop the Spark session
spark.stop()