from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, max, row_number, when, regexp_extract, 
    trim, split, concat_ws, format_number, isnull
)
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType
from typing import List, Tuple, Dict, Optional
from data_cleaners import AcademicDataCleaner, GradeDataCleaner, BaseDataCleaner

class StudentDataProcessor:
    def __init__(self, jdbc_url: str, properties: dict, postgres_driver_path: str):
        self.jdbc_url = jdbc_url
        self.properties = properties
        self.postgres_driver_path = postgres_driver_path
        self.base_cleaner = BaseDataCleaner()
        self.academic_cleaner = AcademicDataCleaner()
        self.grade_cleaner = GradeDataCleaner()
    
    def extract_data(self, table_name: str) -> tuple[DataFrame, SparkSession]:
        """Extract data from PostgreSQL database."""
        spark = SparkSession.builder \
            .appName("Student Data Processing") \
            .config("spark.jars", self.postgres_driver_path) \
            .config("spark.driver.memory", "8g") \
            .config("spark.executor.memory", "8g") \
            .config("spark.executor.memoryOverhead", "2g") \
            .config("spark.driver.memoryOverhead", "2g") \
            .getOrCreate()
        
        df = spark.read.jdbc(
            url=self.jdbc_url,
            table=table_name,
            properties=self.properties,
        )
        
        return df, spark
    
    def process_data(self, df: DataFrame) -> DataFrame:
        """Main processing pipeline."""
        # Basic cleaning
        df = self.base_cleaner.standardize_case(
            df, 
            ['grade_final', 'campus', 'semester', 'schoolyear']
        )
        df = self.base_cleaner.clean_strings(
            df, 
            ['schoolyear', 'semester', 'code', 'description', 'credits',
             'instructor_id', 'instructor', 'srcode', 'fullname', 'campus',
             'college', 'program', 'grade_final', 'grade_reexam', 'status']
        )
        df = self.base_cleaner.remove_null_strings(df, 'program')
        
        # Academic data cleaning
        df = self.academic_cleaner.clean_semesters(df)
        df = self.academic_cleaner.clean_schoolyear(df)
        df = self.academic_cleaner.remove_previous_programs(df)
        
        # Grade processing
        df = self.grade_cleaner.process_grades(df)
        
        # Final transformations
        return self.academic_cleaner.create_yearsem_order(df)