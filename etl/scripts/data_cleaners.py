from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, regexp_extract, lit, coalesce, isnull, 
    format_number, trim, upper, split, concat_ws, row_number, udf,
)
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType
from typing import List
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, BooleanType, DecimalType
from datetime import datetime

class BaseDataCleaner:
    """Handles basic DataFrame cleaning operations."""
    
    @staticmethod
    def standardize_case(df: DataFrame, columns: List[str]) -> DataFrame:
        """Standardize case for specified columns."""
        for column in columns:
            df = df.withColumn(column, upper(col(column)))
        return df
    
    @staticmethod
    def clean_strings(df: DataFrame, columns: List[str]) -> DataFrame:
        """Clean and standardize string columns."""
        for column in columns:
            df = df.withColumn(column, trim(col(column)))
        return df
    
    @staticmethod
    def remove_null_strings(df: DataFrame, column: str) -> DataFrame:
        """Remove rows where column contains 'NULL' string."""
        return df.filter(col(column) != 'NULL')

class AcademicDataCleaner(BaseDataCleaner):
    """Handles academic-specific data cleaning."""
    
    @staticmethod
    def clean_semesters(df: DataFrame) -> DataFrame:
        """Clean and standardize semester values."""
        valid_semesters = ["FIRST", "SECOND", "SUMMER", "SUMMER2"]
        return df.withColumn(
            "semester",
            when(df["semester"].isin("SECOND_X", "SECOND SEMESTER"), "SECOND")
            .when(df["semester"].isin(valid_semesters), df["semester"])
            .otherwise(None)
        ).filter(col("semester").isNotNull())
    
    @staticmethod
    def clean_schoolyear(df: DataFrame) -> DataFrame:
        """Clean and standardize schoolyear values."""
        df = df.withColumn(
            "schoolyear", 
            regexp_extract(col("schoolyear"), r"(\d{4}-\d{4})", 1)
        )
        return df.filter(col("schoolyear").rlike(r"^\d{4}-\d{4}$"))
    
    @staticmethod
    def remove_previous_programs(df: DataFrame) -> DataFrame:
        """Remove records of previous programs for shifters."""
        window_spec = Window.partitionBy("srcode").orderBy(col("schoolyear").desc())
        latest_programs = df.withColumn("row_number", row_number().over(window_spec)) \
            .filter(col("row_number") == 1) \
            .select("srcode", "program", "schoolyear") \
            .withColumnRenamed("srcode", "latest_srcode") \
            .withColumnRenamed("program", "latest_program") \
            .withColumnRenamed("schoolyear", "latest_schoolyear")
        
        return df.join(
            latest_programs,
            (df.srcode == latest_programs.latest_srcode) & 
            (df.program == latest_programs.latest_program),
            "inner"
        ).drop("latest_srcode", "latest_program", "latest_schoolyear")
    
    @staticmethod
    def create_yearsem_order(df: DataFrame) -> DataFrame:
        """Create yearsem ordering and filter valid records."""
        semesters = ["FIRST", "SECOND", "SUMMER"]
        
        df = df.filter(
            (col("semester").isin(semesters)) &
            (col("grade_numeric").isNotNull())
        )
        
        df = df.withColumn(
            "start_year", 
            split(col("schoolyear"), "-")[0].cast("int")
        ).withColumn(
            "sem_order",
            when(col("semester") == "FIRST", 1)
            .when(col("semester") == "SECOND", 2)
            .when(col("semester") == "SUMMER", 3)
        )
        
        df = df.withColumn(
            "yearsem", 
            concat_ws("-", col("schoolyear"), col("semester"))
        )
        
        return df.orderBy("srcode", "start_year", "sem_order")
    
    @staticmethod
    def get_valid_schoolyears(df: DataFrame) -> DataFrame:
        """Filters valid school years starting from 2006 without dropping columns."""
        df = df.withColumn("start_year", split(col("schoolyear"), "-")[0].cast("int"))
        df = df.filter(col("start_year") >= 2006)
        return df
    
    @staticmethod
    def map_program_ids(df: DataFrame, spark, csv_path: str) -> DataFrame:
        # Load CSV directly into a Spark DataFrame
        program_spark_df = spark.read.option("header", "true").csv(csv_path)

        # Ensure proper data types
        program_spark_df = program_spark_df.select(
            col("program").alias("program_name"),
            col("program_id").cast(StringType())
        )

        # Convert mapping to a dictionary
        program_mapping = {row["program_name"]: row["program_id"] for row in program_spark_df.collect()}

        # Define UDF to map program names to program IDs
        get_program_id_udf = udf(lambda program_name: program_mapping.get(program_name, None), StringType())

        # Improved DataFrame validation
        if not isinstance(df, DataFrame):
            raise TypeError("df is not a Spark DataFrame")

        # Apply mapping using UDF
        df = df.withColumn("program_id", get_program_id_udf(col("program")))

        return df

    @staticmethod
    def cast_columns(df: DataFrame, columns: list) -> DataFrame:
        """Cast columns to their correct data type."""   
        data_type_mapping = {
            "string": StringType(),
            "int": IntegerType(),
            "float": FloatType(),
            "double": DoubleType(),
            "boolean": BooleanType(),
            "decimal(5,2)": DecimalType(5,2)
        }

        for column_name, data_type in columns:
            if data_type not in data_type_mapping:
                raise ValueError(f"Unsupported data type: {data_type}. Supported types: {list(data_type_mapping.keys())}")
            
            df = df.withColumn(column_name, col(column_name).cast(data_type_mapping[data_type]))
        
        return df

    
class GradeDataCleaner(BaseDataCleaner):
    """Handles grade-specific cleaning and transformations."""
    
    def __init__(self):
        self.invalid_grades = ["PASSED", "P", "OG", "F"]
        self.valid_numeric_grades = ['1.00','1.25','1.50','1.75','2.00',
                                   '2.25','2.50','2.75','3.00','4.00','5.00']
        self.grade_pattern = r"^\d+\.\d+$" # This accepts only rows that are numeric with decimal

    def process_grades(self, df: DataFrame) -> DataFrame:
        """Process and standardize grades."""
        df = self._filter_invalid_grades(df)
        df = self._create_numeric_grades(df)
        df = self._filter_valid_grades(df)
        return self._create_grade_classification(df)
    
    def _filter_invalid_grades(self, df: DataFrame) -> DataFrame:
        return df.filter(~col("grade_final").isin(self.invalid_grades))
    
    def _create_numeric_grades(self, df):
        return df.withColumn(
            "grade_numeric",
            when(
                (df["grade_reexam"].isNotNull()) & 
                (~df["grade_reexam"].isin(self.invalid_grades)) & 
                (df["grade_reexam"].rlike(self.grade_pattern)),
                df["grade_reexam"] 
            ).when(
                (~df["grade_final"].isin(["DRP", "INC"])) & 
                (df["grade_final"].rlike(self.grade_pattern)),
                df["grade_final"]
            ).when(
                df["grade_final"] == "DRP", 0
            ).otherwise(None)
        ).withColumn("grade_numeric", col("grade_numeric").cast("double")).fillna({"grade_numeric": 5})
    
    def _filter_valid_grades(self, df: DataFrame) -> DataFrame:
        return df.filter(
            when(~df["grade_numeric"].isin(0.0), 
                 df["grade_numeric"].isin([float(x) for x in self.valid_numeric_grades]))
            .otherwise(True)
        ).withColumn("grade_numeric", format_number("grade_numeric", 2))
    
    def _create_grade_classification(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
        "grade_classification",
        when(
            (df["grade_final"].rlike(self.grade_pattern)) & (isnull(df["grade_reexam"]) | df["grade_reexam"].isin(self.invalid_grades)),
            'NORMAL')
        .when(
            (df["grade_final"].isin(["-", "--"])) & (df["grade_reexam"].rlike(self.grade_pattern)),
            'NORMAL')
        .when(
            (df["grade_final"] == "INC") & (df["grade_reexam"].rlike(self.grade_pattern)),
            'INC')
        .when(
            df["grade_final"] == "DRP",
            'DROP')
        .when(
            ((df["grade_final"].rlike(self.grade_pattern)) & (df["grade_reexam"] == "INC")) |
            ((df["grade_final"].rlike(self.grade_pattern)) & (df["grade_reexam"].isin(self.invalid_grades))),
            'INC')
        .when(
            (df["grade_final"].rlike(self.grade_pattern)) & (~df["grade_reexam"].rlike(self.grade_pattern)),
            'NORMAL')
        .when(
            (df["grade_final"].rlike(self.grade_pattern)) & (df["grade_reexam"].rlike(self.grade_pattern)) &
            (col("grade_final").cast("double") > col("grade_reexam").cast("double")),
            'IMPROVED(REEXAM)')
        .when(
            (df["grade_final"].rlike(self.grade_pattern)) & (df["grade_reexam"].rlike(self.grade_pattern)) &
            (col("grade_final").cast("double") < col("grade_reexam").cast("double")),
            'FAILED(REEXAM)')
                
        .otherwise("INVALID")
    )

    def filter_incomplete_grades(self, df: DataFrame) -> DataFrame:
        """Filters out rows where grade_final is INC and reexam is null for the current school year."""
        current_year = datetime.now().year
        df = df.filter(
            ~((col('grade_final') == 'INC') & (col('grade_reexam').isNull()) & (col('schoolyear').contains(str(current_year))))
        )
        df = df.filter(col('grade_numeric') != 0)
        return df
    
    def allow_numerical_data(self, df: DataFrame, columns: str) -> DataFrame:
        return df.withColumn(
            columns, when(col(columns).rlike(self.grade_pattern), col(columns)).otherwise(None)
        )
