import psycopg2, os
from psycopg2.extras import execute_values
from pyspark.sql.utils import AnalysisException

from data_cleaners import BaseDataCleaner, AcademicDataCleaner, GradeDataCleaner
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (    
    from_json, col, when, lit, current_timestamp, 
    concat_ws, split, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, 
    FloatType, TimestampType, DecimalType
)
import sys
from datetime import datetime
sys.path.append('etl/scripts')  # Add scripts directory to path
from scripts.data_cleaners import AcademicDataCleaner, BaseDataCleaner

schema = StructType([
    StructField("id", LongType(), True),  # bigint → LongType()
    StructField("schoolyear", StringType(), True),  # character varying → StringType()
    StructField("semester", StringType(), True),
    StructField("code", StringType(), True),
    StructField("description", StringType(), True),
    StructField("units", IntegerType(), True),  # integer → IntegerType()
    StructField("instructor_id", StringType(), True),
    StructField("instructor_name", StringType(), True),
    StructField("srcode", StringType(), True),
    StructField("fullname", StringType(), True),
    StructField("campus", StringType(), True),
    StructField("college", StringType(), True),
    StructField("program", StringType(), True),
    StructField("major", StringType(), True),
    StructField("yearlevel", StringType(), True),
    StructField("curriculum", StringType(), True),
    StructField("class_section", StringType(), True),
    StructField("grade_final", StringType(), True),
    StructField("grade_reexam", StringType(), True),
    StructField("status", StringType(), True)
])

hudi_schema = StructType([
    StructField("id", IntegerType(), True),  # bigint → LongType()
    StructField("schoolyear", StringType(), True),
    StructField("semester", StringType(), True),
    StructField("code", StringType(), True),
    StructField("description", StringType(), True),
    StructField("units", IntegerType(), True),
    StructField("instructor_id", StringType(), True),
    StructField("instructor_name", StringType(), True),
    StructField("srcode", StringType(), True),
    StructField("fullname", StringType(), True),
    StructField("campus", StringType(), True),
    StructField("college", StringType(), True),
    StructField("program", StringType(), True),
    StructField("major", StringType(), True),
    StructField("yearlevel", StringType(), True),
    StructField("curriculum", StringType(), True),
    StructField("class_section", StringType(), True),
    StructField("grade_final", StringType(), True),
    StructField("grade_reexam", StringType(), True),
    StructField("status", StringType(), True),
    StructField("grade_numeric", DecimalType(5,2), True),  # Changed from FloatType to DecimalType
    StructField("grade_classification", StringType(), True),
    StructField("start_year", IntegerType(), True),  # Changed from StringType to IntegerType
    StructField("year_sem", StringType(), True),
    StructField("program_id", IntegerType(), True),
    StructField("processing_time", TimestampType(), True),
])






# Define transform function
def transform(df, spark):
    """Cleans and processes the extracted data."""
    grade_cleaner = GradeDataCleaner()
    df = BaseDataCleaner.standardize_case(df, ['grade_final', 'campus', 'semester', 'schoolyear'])
    df = AcademicDataCleaner.clean_semesters(df)
    df = BaseDataCleaner.remove_null_strings(df, 'semester')
    
    df = BaseDataCleaner.clean_strings(df, [
        'schoolyear', 'semester', 'code', 'description', 'units', 'instructor_id', 
        'instructor_name', 'srcode', 'fullname', 'campus', 'program', 
        'grade_final', 'grade_reexam', 'status', 'major', 'curriculum', 'class_section'
    ])
    
    df = AcademicDataCleaner.clean_schoolyear(df)
    df = grade_cleaner.process_grades(df)
    df = BaseDataCleaner.remove_null_strings(df, 'program')
    
    df = grade_cleaner.allow_numerical_data(df, "grade_reexam")

    df = AcademicDataCleaner.cast_columns(df, [("id", "int"), ("units", "int"), 
                                               ("grade_numeric", "decimal(5,2)")])
    
    df = grade_cleaner.filter_incomplete_grades(df)

    df = AcademicDataCleaner.get_valid_schoolyears(df)

    df = AcademicDataCleaner.create_yearsem_order(df)
    df = AcademicDataCleaner.map_program_ids(df, spark, "program_with_id.csv")

    # Add processing_time column
    #df = df.withColumn("processing_time", current_timestamp())
    
    return df

# Configure Spark with Kafka and Hudi dependencies
spark = (SparkSession.builder
         .appName("KafkaToHudiProcessor")
         .master("local[*]")
         .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
         .config('spark.jars.packages', 
                ('org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,'  # Match your Spark version
                 'org.apache.hudi:hudi-spark3.3-bundle_2.12:0.14.0,'
                 'org.postgresql:postgresql:42.7.5'))  # PostgreSQL driver
         .config('spark.hadoop.javax.jdo.option.ConnectionDriverName', 'org.postgresql.Driver')
         .config("spark.sql.extensions", "org.apache.hudi.spark3.sql.HoodieSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
         .config("spark.streaming.stopGracefullyOnShutdown", "true")
         .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_warehouse/checkpoints")
         # Add these network configurations
         .config("spark.driver.host", "localhost")
         .config("spark.driver.bindAddress", "localhost")
         .config("spark.network.timeout", "600s")
         .config("spark.local.dir", "/tmp/spark-temp")
         .config("spark.sql.warehouse.dir", "/tmp/spark_warehouse")
         .getOrCreate())

# Optimized Kafka configuration
kafka_options = {
    "kafka.bootstrap.servers": "localhost:29092",
    "subscribe": "source.public.raw_grades",
    "startingOffsets": "earliest",
    "kafka.security.protocol": "PLAINTEXT",
    "failOnDataLoss": "false",
    "fetchOffset.numRetries": "5",
    "fetch.max.wait.ms": "5000",
    "maxOffsetsPerTrigger": "50000" # Adjust based on your needs
}

# Read from Kafka with improved configuration
kafka_df = (spark
            .readStream
            .format("kafka")
            .options(**kafka_options)
            .load())

# Parse JSON with error handling
parsed_df = (kafka_df
             .select(from_json(
                 col("value").cast("string"),
                 schema
             ).alias("data"))
             .select("data.*"))

# Enhanced Hudi options for update handling
hudi_options = {
    'hoodie.table.name': 'grades',
    'hoodie.datasource.write.recordkey.field': 'id',
    'hoodie.datasource.write.partitionpath.field': 'schoolyear',
    'hoodie.datasource.write.table.name': 'grades',
    'hoodie.datasource.write.operation': 'upsert',  # Use upsert for updates
    'hoodie.datasource.write.precombine.field': 'processing_time',  # Change back to processing_time
    'hoodie.upsert.shuffle.parallelism': '2',
    'hoodie.insert.shuffle.parallelism': '2',
    'hoodie.bulkinsert.shuffle.parallelism': '2',
    'hoodie.cleaner.policy': 'KEEP_LATEST_COMMITS',
    'hoodie.cleaner.commits.retained': '2',
    'hoodie.write.concurrency.mode': 'optimistic_concurrency_control',
    'hoodie.write.lock.provider': 'org.apache.hudi.client.transaction.lock.InProcessLockProvider',
}

def initialize_hudi_table():
    table_path = "/tmp/spark_warehouse/from_kafka"
    
    if not os.path.exists(table_path) or not os.listdir(table_path):
        print("Hudi table does not exist. Initializing now...")

        empty_df = spark.createDataFrame([], hudi_schema)
        empty_df.write.format("hudi") \
            .options(**hudi_options) \
            .mode("overwrite") \
            .save(table_path)
        
        print("Hudi table initialized successfully.")
    else:
        try:
            spark.read.format("hudi").load(table_path).show(1)
            print("Hudi table exists. Proceeding...")
        except AnalysisException:
            print("Error reading Hudi table. Reinitializing...")
            empty_df = spark.createDataFrame([], hudi_schema)
            empty_df.write.format("hudi") \
                .options(**hudi_options) \
                .mode("overwrite") \
                .save(table_path)
        
        print("Hudi table initialized successfully.")

def write_to_postgres(transformed_df):
    """
    Writes the transformed Spark DataFrame to PostgreSQL using Psycopg2.
    Uses batch insert/update for efficiency.
    """
    
    db_params = {
        "dbname": "caist_db_v4",
        "user": "postgres",
        "password": "postgres",
        "host": "192.168.20.11",
        "port": "5432"
    }
    
    # Collect data from Spark DataFrame into a list of tuples
    data = [tuple(row) for row in transformed_df.collect()]
    
    if not data:
        print("No data to write.")
        return
    
    insert_query = """
        INSERT INTO processed_grades_store (
            id, schoolyear, semester, code, description, units, instructor_id, 
            instructor_name, srcode, fullname, campus, college, program, major, yearlevel, 
            curriculum, class_section, grade_final, grade_reexam, status, 
            grade_numeric, grade_classification, start_year, year_sem, program_id
        ) VALUES %s 
        ON CONFLICT (id) DO UPDATE SET 
            schoolyear = EXCLUDED.schoolyear,
            semester = EXCLUDED.semester,
            code = EXCLUDED.code,
            description = EXCLUDED.description,
            units = EXCLUDED.units,
            instructor_id = EXCLUDED.instructor_id,
            instructor_name = EXCLUDED.instructor_name,
            srcode = EXCLUDED.srcode,
            fullname = EXCLUDED.fullname,
            campus = EXCLUDED.campus,
            program = EXCLUDED.program,
            major = EXCLUDED.major,
            yearlevel = EXCLUDED.yearlevel,
            curriculum = EXCLUDED.curriculum,
            class_section = EXCLUDED.class_section,
            grade_final = EXCLUDED.grade_final,
            grade_reexam = EXCLUDED.grade_reexam,
            status = EXCLUDED.status,
            grade_numeric = EXCLUDED.grade_numeric,
            grade_classification = EXCLUDED.grade_classification,
            start_year = EXCLUDED.start_year,
            year_sem = EXCLUDED.year_sem,
            program_id = EXCLUDED.program_id;
    """
    
    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_query, data)
                conn.commit()
                print(f"Inserted/Updated {len(data)} records into PostgreSQL.")
    except Exception as e:
        print(f"Error writing to PostgreSQL: {e}")


def process_batch(batch_df, batch_id):
    try:
        if batch_df.rdd.isEmpty():
            print(f"Batch {batch_id} is empty, skipping...")
            return
        
        batch_df.cache()
        start_time = datetime.now()

        # Ensure the Hudi table is initialized before writing
        initialize_hudi_table()
        
        # Apply transformations
        transformed_df = transform(batch_df, spark)
        transformed_df.cache()
        
        try:
            # Write to Hudi
            hudi_df = transformed_df.withColumn("processing_time", current_timestamp())
            hudi_df.write \
                .format("hudi") \
                .options(**hudi_options) \
                .mode("append") \
                .save("/tmp/spark_warehouse/from_kafka")
            
            # Write to PostgreSQL using Psycopg2
            write_to_postgres(transformed_df)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            print(f"Batch {batch_id} processed in {duration:.2f} seconds")
        
        finally:
            transformed_df.unpersist()
            hudi_df.unpersist()
            batch_df.unpersist()
            
    except Exception as e:
        print(f"Error processing batch {batch_id}: {str(e)}")
        raise e


# Start streaming with monitoring
query = (parsed_df.writeStream
         .foreachBatch(process_batch)
         .outputMode("update")
         .option("checkpointLocation", "/tmp/spark_warehouse/checkpoints")
         .trigger(processingTime="0 seconds")
         .start())

# Enhanced monitoring
while query.isActive:
    try:
        stats = query.status
        progress = query.recentProgress
        
        print(f"""
        Stream Status: {stats['message']}
        Data Available: {stats.get('isDataAvailable', False)}
        Trigger Active: {stats.get('isTriggerActive', False)}
        Records Processed: {progress[-1]['numInputRows'] if progress else 0}
        """)
        
        query.awaitTermination(30)  # Check every 2 minutes
        
    except Exception as e:
        print(f"Monitoring error: {str(e)}")

