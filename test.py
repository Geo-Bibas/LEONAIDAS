from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder.appName("TestPySparkSetup").getOrCreate()

# Test DataFrame
data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
columns = ["ID", "Name"]
df = spark.createDataFrame(data, columns)

# Show DataFrame
df.show()

# Stop Spark
spark.stop()