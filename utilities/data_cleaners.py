from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, regexp_extract, lit, coalesce, isnull, 
    format_number, trim, upper, split, concat_ws, row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType
from typing import List

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
    
    def clean_semesters(self, df: DataFrame) -> DataFrame:
        """Clean and standardize semester values."""
        valid_semesters = ["FIRST", "SECOND", "SUMMER", "SUMMER2"]
        return df.withColumn(
            "semester",
            when(df["semester"].isin("SECOND_X", "SECOND SEMESTER"), "SECOND")
            .when(df["semester"].isin(valid_semesters), df["semester"])
            .otherwise(None)
        ).filter(col("semester").isNotNull())
    
    def clean_schoolyear(self, df: DataFrame) -> DataFrame:
        """Clean and standardize schoolyear values."""
        df = df.withColumn(
            "schoolyear", 
            regexp_extract(col("schoolyear"), r"(\d{4}-\d{4})", 1)
        )
        return df.filter(col("schoolyear").rlike(r"^\d{4}-\d{4}$"))
    
    def remove_previous_programs(self, df: DataFrame) -> DataFrame:
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
    
    def create_yearsem_order(self, df: DataFrame) -> DataFrame:
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

class GradeDataCleaner(BaseDataCleaner):
    """Handles grade-specific cleaning and transformations."""
    
    def __init__(self):
        self.invalid_grades = ["PASSED", "P", "OG", "F"]
        self.valid_numeric_grades = ['1.00','1.25','1.50','1.75','2.00',
                                   '2.25','2.50','2.75','3.00','4.00','5.00']
    
    def process_grades(self, df: DataFrame) -> DataFrame:
        """Process and standardize grades."""
        df = self._filter_invalid_grades(df)
        df = self._create_numeric_grades(df)
        df = self._filter_valid_grades(df)
        df = self._create_grade_classification(df)
        return self._cast_numeric_columns(df)
    
    def _filter_invalid_grades(self, df: DataFrame) -> DataFrame:
        return df.filter(~col("grade_final").isin(self.invalid_grades))
    
    def _create_numeric_grades(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "grade_numeric",
            when(
                (df["grade_reexam"].isNotNull()) & 
                (~df["grade_reexam"].isin(self.invalid_grades)) & 
                (df["grade_reexam"].rlike(r"^\d+\.?\d*$")),
                regexp_extract(df["grade_reexam"], r"(\d+\.?\d*)", 1)
            ).when(
                (~df["grade_final"].isin("DRP", "INC")) & 
                (df["grade_final"].rlike(r"^\d+\.?\d*$")),
                regexp_extract(df["grade_final"], r"(\d+\.?\d*)", 1)
            ).when(
                (df["grade_final"] == "INC") & 
                (df["grade_reexam"].isNotNull()) & 
                (~df["grade_reexam"].isin(self.invalid_grades)) & 
                (df["grade_reexam"].rlike(r"^\d+\.?\d*$")),
                regexp_extract(df["grade_reexam"], r"(\d+\.?\d*)", 1)
            ).when(df["grade_final"] == "DRP", 0
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
                (df["grade_final"].rlike(r"^\d+\.\d+$")) & 
                (isnull(df["grade_reexam"]) | df["grade_reexam"].isin(self.invalid_grades)),
                'NORMAL'
            ).when(
                (df["grade_final"].isin(["-", "--"])) & 
                (df["grade_reexam"].rlike(r"^\d+\.\d+$")),
                'NORMAL'
            ).when(
                (df["grade_final"] == "INC") & 
                (df["grade_reexam"].rlike(r"^\d+\.\d+$")),
                'INC'
            ).when(
                df["grade_final"] == "DRP",
                'DROP'
            ).when(
                ((df["grade_final"].rlike(r"^\d+\.\d+$")) & 
                 (df["grade_reexam"] == "INC")) |
                ((df["grade_final"].rlike(r"^\d+\.\d+$")) & 
                 (df["grade_reexam"].isin(self.invalid_grades))),
                'INC'
            ).when(
                (df["grade_final"].rlike(r"^\d+\.\d+$")) & 
                (~df["grade_reexam"].rlike(r"^\d+\.\d+$")),
                'NORMAL'
            ).when(
                (df["grade_final"].rlike(r"^\d+\.\d+$")) & 
                (df["grade_reexam"].rlike(r"^\d+\.\d+$")) &
                (col("grade_final").cast("double") > 
                 col("grade_reexam").cast("double")),
                'IMPROVED(REEXAM)'
            ).when(
                (df["grade_final"].rlike(r"^\d+\.\d+$")) & 
                (df["grade_reexam"].rlike(r"^\d+\.\d+$")) &
                (col("grade_final").cast("double") < 
                 col("grade_reexam").cast("double")),
                'FAILED(REEXAM)'
            ).otherwise("INVALID")
        )
    
    def _cast_numeric_columns(self, df: DataFrame) -> DataFrame:
        return df.withColumn("credits", df.credits.cast("int"))\
                .withColumn("grade_numeric", df.grade_numeric.cast(DecimalType(5, 2))) 