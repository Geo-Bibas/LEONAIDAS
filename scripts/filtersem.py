from pyspark.sql.functions import DataFrame, split, col, when, concat_ws

class SchoolYearProcessor:
    def __init__(self, df: DataFrame, column: dict):
        """
        Initializes the processor with the raw DataFrame.

        :param df: Spark DataFrame containing school year data.
        """
        self.df = df
        self.column = column
        self.valid_schoolyears = self._extract_valid_years()
        
    def _extract_valid_years(self):
        """
        Extracts valid school years from the DataFrame.
        
        :return: List of valid school year strings.
        """
        distinct_years_df = self.df.select(self.column["schoolyear"]).distinct()
        distinct_years_df = distinct_years_df.withColumn(self.column["startyear"], split(col(self.column["schoolyear"]), "-")[0].cast("int"))
        valid_years_df = distinct_years_df.orderBy(self.column["startyear"])
        
        return [row.schoolyear for row in valid_years_df.collect()]

    def merge_year_sem(self):
        """
        Filters and processes the DataFrame to merge year and semester data.

        :return: Processed Spark DataFrame with `yearsem` and sorted records.
        """
        semesters = ["FIRST", "SECOND", "SUMMER"]

        filtered_df = self.df.filter(
            (col(self.column["schoolyear"]).isin(self.valid_schoolyears)) &
            (col(self.column["semester"]).isin(semesters)) &
            (col(self.column["grades"]).isNotNull())
        )

        filtered_df = filtered_df.withColumn(self.column["startyear"], split(col(self.column["schoolyear"]), "-")[0].cast("int"))
        filtered_df = filtered_df.withColumn(
            self.column["sem_order"],
            when(col(self.column["semester"]) == "FIRST", 1)
            .when(col(self.column["semester"]) == "SECOND", 2)
            .when(col(self.column["semester"]) == "SUMMER", 3)
        )

        filtered_df = filtered_df.withColumn(self.column["yearsem"], concat_ws("-", col(self.column["schoolyear"]), col(self.column["semester"])))

        output_df = filtered_df.select(
            self.column["srcode"], self.column["yearsem"], self.column["description"], self.column["grades"], "program"
        ).orderBy(self.column["srcode"], self.column["startyear"], self.column["sem_order"], self.column["description"])

        return output_df







