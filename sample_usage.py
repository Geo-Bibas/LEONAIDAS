"""
Sample Usage of SchoolYearProcessor Class
"""
from pyspark.sql import SparkSession
from scripts.filtersem import SchoolYearProcessor

#jdbc_url = "jdbc:postgresql://192.168.20.11:5432/demo_db"
jdbc_url = "jdbc:postgresql://localhost:5432/local_student_grades"
properties = {
    "user": "postgres", 
    "password": "password",  
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
raw_df, spark = extract(jdbc_url, "raw_student_grades", properties, postgres_driver_path)

# Initialize the class with the raw DataFrame
processor = SchoolYearProcessor(raw_df)

# Process the data
output_df = processor.merge_year_sem()

# Show the results
output_df.show()
output_df.printSchema()
output_df.count()
