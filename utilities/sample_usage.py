from student_data_processor import StudentDataProcessor
import os

def main():
    # Initialize the processor
    processor = StudentDataProcessor(
        jdbc_url="jdbc:postgresql://localhost:5432/local_student_grades",
        properties={
            "user": "postgres",
            "password": "password",
            "driver": "org.postgresql.Driver",
            "fetchsize": "10000"
        },
        postgres_driver_path= "C:\postgresql-42.7.5.jar"
    )
    
    # Extract data
    df, spark = processor.extract_data("raw_student_grades")
    
    try:
        # Process the data through the pipeline
        processed_df = processor.process_data(df)
        
        # Show sample results
        print("\nSample of processed data:")
        processed_df.show(5)
        
        # Show schema
        print("\nProcessed data schema:")
        processed_df.printSchema()
        
        # Show some statistics
        print("\nData statistics:")
        print(f"Total records: {processed_df.count()}")
        print("\nGrade distribution:")
        processed_df.groupBy("grade_classification").count().show()

        processed_df.printSchema()
        processed_df.show(5)
        
        
        
    finally:
        print("TESTING SUCCESSFUL")
        # Clean up
        spark.stop()

if __name__ == "__main__":
    main()