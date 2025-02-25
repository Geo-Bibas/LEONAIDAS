from pyspark.sql import SparkSession

jdbc_url = "jdbc:postgresql://192.168.20.11:5432/demo_db"
properties = {
    "user": "postgres", 
    "password": "postgres",  
    "driver": "org.postgresql.Driver",
    "fetchsize": "10000"
}

#DOWNLOAD FROM ORACLE
postgres_driver_path = "C:\postgresql-42.7.5.jar"

def extract(jdbc_url, table_name, properties, postgres_driver_path):
    """ Extract data from PostgreSQL database using Spark."""
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Postgres Connection") \
        .config("spark.jars", postgres_driver_path) \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.memoryOverhead", "2g") \
        .config("spark.driver.memoryOverhead", "2g") \
        .getOrCreate()

    # Extracts data from PostgreSQL database
    df = spark.read.jdbc(
        url=jdbc_url,
        table=table_name,
        properties=properties,
    )
    
    return df, spark

# Extract data once
raw_df, spark = extract(jdbc_url, "filtered_data_with_id", properties, postgres_driver_path)

# Check the number of rows
row_count = raw_df.count()
print(f'Number of rows: {row_count}')

# Print schema
raw_df.printSchema()