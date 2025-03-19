from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, when, lit, current_timestamp, 
    concat_ws, split, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, TimestampType
)
from data_cleaners import AcademicDataCleaner, BaseDataCleaner

def create_spark_session():
    return (SparkSession.builder
            .appName("KafkaToHudiProcessor")
            .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
            .config('spark.sql.extensions', 'org.apache.hudi.spark3.sql.HoodieSparkSessionExtension')
            .config('className', 'org.apache.hudi.spark3.sql.HoodieSparkSessionExtension')
            .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog')
            .getOrCreate())

def get_grade_schema():
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("schoolyear", StringType(), True),
        StructField("semester", StringType(), True),
        StructField("code", StringType(), True),
        StructField("description", StringType(), True),
        StructField("credits", IntegerType(), True),
        StructField("instructor_id", StringType(), True),
        StructField("instructor_name", StringType(), True),
        StructField("srcode", StringType(), True),
        StructField("fullname", StringType(), True),
        StructField("campus", StringType(), True),
        StructField("college", StringType(), True),
        StructField("program", StringType(), True),
        StructField("yearlevel", StringType(), True),
        StructField("class_section", StringType(), True),
        StructField("grade_final", FloatType(), True),
        StructField("grade_reexam", StringType(), True),
        StructField("status", StringType(), True)
    ])

def transform(df):
    """
    Transform the input DataFrame according to business rules.
    
    Args:
        df: Input DataFrame with raw data
        
    Returns:
        Transformed DataFrame
    """
    # Initialize cleaners
    academic_cleaner = AcademicDataCleaner()
    base_cleaner = BaseDataCleaner()
    
    # Add processing timestamp
    df = df.withColumn("processing_time", current_timestamp())
    
    # Clean and standardize string fields
    string_columns = ["semester", "code", "description", "instructor_name", 
                     "fullname", "campus", "college", "program", "yearlevel", 
                     "class_section", "status"]
    df = base_cleaner.clean_strings(df, string_columns)
    
    # Standardize case for specific fields
    case_sensitive_columns = ["semester", "campus", "college", "program", 
                            "yearlevel", "status"]
    df = base_cleaner.standardize_case(df, case_sensitive_columns)
    
    # Clean academic-specific fields
    df = academic_cleaner.clean_semesters(df)
    df = academic_cleaner.clean_schoolyear(df)
    
    # Process grades
    df = df.withColumn(
        "grade_final",
        when(col("grade_final").isNull(), None)
        .when(col("grade_final") == "-", None)
        .otherwise(col("grade_final").cast(FloatType()))
    )
    
    df = df.withColumn(
        "grade_reexam",
        when(col("grade_reexam").isNull(), None)
        .when(col("grade_reexam") == "-", None)
        .otherwise(col("grade_reexam").cast(FloatType()))
    )
    
    # Update status based on grades
    df = df.withColumn(
        "status",
        when(col("grade_final").isNull(), "PENDING")
        .when(col("grade_final") >= 3.0, "FAILED")
        .when(col("grade_final") <= 3.0, "PASSED")
        .otherwise("UNKNOWN")
    )
    
    return df

def get_hudi_options(table_name):
    return {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': 'id',
        'hoodie.datasource.write.partitionpath.field': 'schoolyear',
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'processing_time',
        'hoodie.upsert.shuffle.parallelism': '2',
        'hoodie.insert.shuffle.parallelism': '2'
    }

def process_stream():
    spark = create_spark_session()
    
    # Read from Kafka
    stream_df = (spark
                 .readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", "localhost:9092")
                 .option("subscribe", "grades")
                 .option("startingOffsets", "earliest")
                 .load())
    
    # Parse JSON and apply schema
    parsed_df = stream_df.select(
        from_json(col("value").cast("string"), get_grade_schema()).alias("data")
    ).select("data.*")
    
    # Apply transformations using the sample ETL transform function
    transformed_df = transform(parsed_df)
    
    # Write to Hudi
    query = (transformed_df.writeStream
            .format("hudi")
            .options(**get_hudi_options("grades"))
            .option("checkpointLocation", "/tmp/hudi_checkpoints")
            .outputMode("append")
            .trigger(processingTime="2 minutes")
            .start())
    
    query.awaitTermination()

if __name__ == "__main__":
    process_stream() 