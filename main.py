from pyspark.sql import SparkSession
from scripts.filtersem import SchoolYearProcessor
from utilities.cleaner import DataCleaner
import shutil
import os

class Main:
    def __init__(self, jdbc_url, table_name, properties, postgres_driver_path):
        """
        Initialize the Main class with database connection details.
        """
        self.jdbc_url = jdbc_url
        self.properties = properties
        self.postgres_driver_path = postgres_driver_path
        self.spark = None
        self.raw_df = None
        self.column = {
            "srcode": "srcode",
            "schoolyear": "schoolyear",
            "startyear": "start_year",
            "semester": "semester",
            "sem_order": "sem_order",
            "grades": "grade_numeric",
            "program": "program",
            "description": "description",
            "yearsem" : "yearsem",
            "grade_final": "grade_final",
            "campus": "campus",
            "grade_reexam": "grade_reexam",
            "grade_classification": "grade_classification",
            "credits": "credits"
            }
        self.table_name = table_name

    def initialize_spark_session(self):
        """Initialize and configure the Spark session."""
        self.spark = SparkSession.builder \
            .appName("Postgres Connection") \
            .config("spark.driver.extraClassPath", self.postgres_driver_path) \
            .config("spark.driver.memory", "8g") \
            .config("spark.executor.memory", "8g") \
            .config("spark.executor.memoryOverhead", "2g") \
            .config("spark.driver.memoryOverhead", "2g") \
            .getOrCreate()

        # Set logging level to ERROR to reduce logs
        self.spark.sparkContext.setLogLevel("ERROR")
    
    def extract_data(self):
        """Extract data from PostgreSQL database using Spark."""
        if self.spark is None:
            self.initialize_spark_session()
        
        self.raw_df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=self.table_name,
            properties=self.properties,
        )
        return self.raw_df
    
    def process_data(self):
        """Process the extracted data using SchoolYearProcessor."""
        if self.raw_df is None:
            raise ValueError("Data has not been extracted. Run extract_data() first.")
        
        processor = SchoolYearProcessor(self.raw_df, self.column)
        return processor.merge_year_sem()
    
    def run(self):
        """Run the full extraction and processing pipeline."""
        try:
            # Step 1: Extract raw data
            self.extract_data()
            
            # Step 2: Clean data using DataCleaner
            cleaner = DataCleaner(self.raw_df, self.column, self.table_name, self.spark)
            cleaned_df = cleaner.clean()

            # Step 3: Process school year and semester
            processor = SchoolYearProcessor(cleaned_df, self.column)
            final_df = processor.merge_year_sem()

            # Display final output
            final_df.show()
            
        except Exception as e:
            print(f"Error: {e}")
        finally:
            if self.spark:
                self.spark.stop()
                print("Spark session stopped successfully.")
                
                # Manually clean up the Spark temp directory
                temp_dir = "C:\\spark-temp"
                try:
                    if os.path.exists(temp_dir):
                        shutil.rmtree(temp_dir)
                        print("Spark temp directory cleaned up successfully.")
                except Exception as e:
                    print(f"Warning: Failed to delete Spark temp directory: {e}")

if __name__ == "__main__":
    jdbc_url = "jdbc:postgresql://192.168.20.11:5432/demo_db"
    properties = {
        "user": "postgres", 
        "password": "postgres",  
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000"
    }
    
    table_name = {
            "raw_data": "sample_data_ai_with_id",
            "cleaned_data": "filtered_data_with_id"
    }
    
    postgres_driver_path = "C:\\postgresql-42.7.5.jar"
    
    processor = Main(jdbc_url, table_name["raw_data"], properties, postgres_driver_path)
    processor.run()
