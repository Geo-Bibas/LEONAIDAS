from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import os

def create_spark_session():
    """Initialize Spark session with Hudi configurations"""
    return (SparkSession.builder
            .appName("HudiWriteTest")
            .master("local[*]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.3-bundle_2.12:0.14.0")
            .config("spark.sql.extensions", "org.apache.hudi.spark3.sql.HoodieSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
            .config("spark.driver.host", "localhost")  # Add this
            .config("spark.driver.bindAddress", "localhost")  # Add this  
            .config("spark.network.timeout", "600s")  # Add this
            .getOrCreate())

def test_hudi_write():
    """Test writing a simple dataset to Hudi"""
    spark = create_spark_session()
    
    # Create a sample dataframe 
    data = [
        (1, "John", "Engineering"),
        (2, "Jane", "Marketing"), 
        (3, "Bob", "Sales")
    ]
    df = spark.createDataFrame(data, ["id", "name", "department"])
    
    # Add timestamp for Hudi
    df = df.withColumn("ts", current_timestamp())
    
    # Define Hudi options
    hudi_options = {
        'hoodie.table.name': 'test_table',
        'hoodie.datasource.write.recordkey.field': 'id',
        'hoodie.datasource.write.precombine.field': 'ts',
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.upsert.shuffle.parallelism': '2',
        'hoodie.insert.shuffle.parallelism': '2'
    }
    
    try:
        # Write to Hudi
        table_path = "C:/tmp/spark_warehouse/test_table"
        df.write.format("hudi") \
            .options(**hudi_options) \
            .mode("overwrite") \
            .save(table_path)
        
        # Verify by reading back
        read_df = spark.read.format("hudi").load(table_path)
        
        # Display table schema and data
        print("\nHudi Table Schema:")
        read_df.printSchema()
        
        print("\nHudi Table Contents:")
        read_df.show(truncate=False)
        
        print("\nTotal Records:", read_df.count())
        return True
        
    except Exception as e:
        print(f"Error writing Hudi table: {str(e)}")
        return False
    finally:
        spark.stop()

if __name__ == "__main__":
    success = test_hudi_write()
    if success:
        print("\nHudi write test completed successfully!")
    else:
        print("\nHudi write test failed!")