from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, upper, regexp_extract, when, trim, format_number, isnull
from pyspark.sql.types import DecimalType

class CaseFormatter:
    """Handles case formatting for specific columns."""
    def __init__(self, df: DataFrame, column: dict):
        self.df = df
        self.column = column

    def format_case(self):
        return self.df.withColumn(self.column["grade_final"], upper(col(self.column["grade_final"])))\
                      .withColumn(self.column["campus"], upper(col(self.column["campus"])))\
                      .withColumn(self.column["semester"], upper(col(self.column["semester"])))\
                      .withColumn(self.column["schoolyear"], upper(col(self.column["schoolyear"])))

class SemesterCleaner:
    """Cleans and standardizes the semester column."""
    def __init__(self, df: DataFrame, column: dict):
        self.df = df
        self.column = column

    def clean(self):
        valid_semesters = ["FIRST", "SECOND", "SUMMER", "SUMMER2"]
        df = self.df.withColumn(
            self.column["semester"],
            when(col(self.column["semester"]).isin("SECOND_X", "SECOND SEMESTER"), "SECOND")
            .when(col(self.column["semester"]).isin(valid_semesters), col(self.column["semester"]))
            .otherwise(None)
        ).filter(col(self.column["semester"]).isNotNull())
        return df

class WhitespaceRemover:
    """Removes unnecessary whitespace from relevant columns."""
    def __init__(self, df: DataFrame):
        self.df = df

    def remove(self):
        return self.df.select([trim(col(c)).alias(c) for c in self.df.columns])

class SchoolYearCleaner:
    """Cleans and validates the schoolyear column."""
    def __init__(self, df: DataFrame, column: dict):
        self.df = df
        self.column = column

    def clean(self):
        df = self.df.withColumn(
            self.column["schoolyear"], regexp_extract(col(self.column["schoolyear"]), r"(\d{4}-\d{4})", 1)
        )
        df = df.filter(col(self.column["schoolyear"]).rlike(r"^\d{4}-\d{4}$"))
        return df

class GradeProcessor:
    """Processes grades, classifies them, and filters invalid values."""
    def __init__(self, df: DataFrame, column: dict):
        self.df = df
        self.column = column

    def process(self):
        invalid_grades = ["PASSED", "P", "OG", "F"]
        df = self.df.filter(~col(self.column["grade_final"]).isin(invalid_grades))

        df = df.withColumn(
            self.column["grades"],
            when(
                (col(self.column["grade_reexam"]).isNotNull()) & (~col(self.column["grade_reexam"]).isin(invalid_grades)) & (col(self.column["grade_reexam"]).rlike(r"^\d+\.?\d*$")),
                regexp_extract(col(self.column["grade_reexam"]), r"(\d+\.?\d*)", 1)
            ).when(
                (~col(self.column["grade_final"]).isin("DRP", "INC")) & (col(self.column["grade_final"]).rlike(r"^\d+\.?\d*$")),
                regexp_extract(col(self.column["grade_final"]), r"(\d+\.?\d*)", 1)
            ).when(
                (col(self.column["grade_final"]) == "INC") & (col(self.column["grade_reexam"]).isNotNull()) & (~col(self.column["grade_reexam"]).isin(invalid_grades)) & (col(self.column["grade_reexam"]).rlike(r"^\d+\.?\d*$")),
                regexp_extract(col(self.column["grade_reexam"]), r"(\d+\.?\d*)", 1)
            ).when(col(self.column["grade_final"]) == "DRP", 0)
            .otherwise(None)
        ).withColumn(self.column["grades"], col(self.column["grades"]).cast("double")).fillna({self.column["grades"]: 5})

        valid_numeric_grades = [float(x) for x in ['1.00','1.25','1.50','1.75','2.00','2.25','2.50','2.75','3.00','4.00','5.00']]
        df = df.filter(
            when(~col(self.column["grades"]).isin(0.0), col(self.column["grades"]).isin(valid_numeric_grades)).otherwise(True)
        )

        df = df.withColumn(self.column["grades"], format_number(col(self.column["grades"]), 2))
        return df

class Finalizer:
    """Finalizes data by converting types and preparing for output."""
    def __init__(self, df: DataFrame, column: dict):
        self.df = df
        self.column = column

    def finalize(self):
        df = self.df.withColumn(
            self.column["grade_reexam"], when(col(self.column["grade_reexam"]).rlike("^[0-9]*\.?[0-9]+$"), col(self.column["grade_reexam"])).otherwise(None)
        )
        df = df.withColumn(self.column["credits"], col(self.column["credits"]).cast("int"))\
               .withColumn(self.column["grades"], col(self.column["grades"]).cast(DecimalType(5, 2)))
        return df

class DataCleaner:
    """Main pipeline that orchestrates all cleaning steps."""
    def __init__(self, df: DataFrame, column: dict, table_name: dict, spark: SparkSession):
        self.df = df
        self.column = column
        self.table_name = table_name
        self.spark = spark

    def clean(self):
        """Runs all cleaning steps sequentially."""
        self.df = CaseFormatter(self.df, self.column).format_case()
        self.df = SemesterCleaner(self.df, self.column).clean()
        self.df = WhitespaceRemover(self.df).remove()
        self.df = SchoolYearCleaner(self.df, self.column).clean()
        self.df = GradeProcessor(self.df, self.column).process()
        self.df = Finalizer(self.df, self.column).finalize()
        return self.df
