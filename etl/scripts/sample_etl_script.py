from student_data_processor import StudentDataProcessor
from data_cleaners import BaseDataCleaner, AcademicDataCleaner, GradeDataCleaner

def extract():
    """Extracts data from PostgreSQL using StudentDataProcessor."""
    jdbc_url = "jdbc:postgresql://192.168.20.11:5432/demo_db"
    properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000"
    }

    processor = StudentDataProcessor(
        jdbc_url=jdbc_url,
        properties=properties,
        postgres_driver_path=r"C:\postgresql-42.7.5.jar"
    )

    df, spark = processor.extract_data('grades_with_updated_id')

    return df, spark, jdbc_url, properties  


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
    df = AcademicDataCleaner.map_program_ids(df, spark, r"C:\LEONAIDAS\program_with_id.csv")
    
    return df

def load(df, jdbc_url, properties):
    """Loads the dataframe into the database"""
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "test_load_grades_with_updated_id") \
        .option("user", properties["user"]) \
        .option("password", properties["password"]) \
        .option("driver", properties["driver"]) \
        .mode("overwrite") \
        .save()


if __name__ == "__main__":
    df, spark, jdbc_url, properties = extract()
    df = transform(df, spark)
    df.printSchema()
    df.show()
    print(f'Row Count: {df.count()}')
    load(df, jdbc_url, properties)  
