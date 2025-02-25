from typing import *

from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Hudi Basics") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.1") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

table_name = "hudi_trips_cow"
base_path = "C:/tmp"
quickstart_utils = sc._jvm.org.apache.hudi.QuickstartUtils
dataGen = quickstart_utils.DataGenerator()

inserts = sc._jvm.org.apache.hudi.QuickstartUtils.convertToStringList(dataGen.generateInserts(10))


def create_df():
    df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
    return df


def write_data():
    df = create_df()

    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.recordkey.field': 'uuid',
        'hoodie.datasource.write.partitionpath.field': 'partitionpath',
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'ts',
        'hoodie.upsert.shuffle.parallelism': 2,
        'hoodie.insert.shuffle.parallelism': 2
    }

    df.write.format("hudi") \
        .options(**hudi_options) \
        .mode("overwrite") \
        .save(base_path)


def query_data():
    trips_snapshot_df = spark.read \
        .format("hudi") \
        .load(base_path)

    trips_snapshot_df \
        .where(col("fare") > 20.0) \
        .select(
            col("fare"),
            col("begin_lon"),
            col("begin_lat"),
            col("ts")) \
        .show(truncate=False)

    trips_snapshot_df \
        .select(
            col("_hoodie_commit_time"),
            col("_hoodie_record_key"),
            col("_hoodie_partition_path"),
            col("rider"),
            col("driver"),
            col("fare")) \
        .show(truncate=False)


def time_travel_query():
    # method 1
    # replace the value for as.of.instant with your _hoodie_commit_time
    spark.read \
        .format("hudi") \
        .option("as.of.instant", "20230919095758730") \
        .load(base_path) \
        .show()

    # method 2
    spark.read \
        .format("hudi") \
        .option("as.of.instant", "2023-09-19 09:57:58.730") \
        .load(base_path) \
        .show()

    # method 3
    # clock starts at 00:00:00.000
    spark.read \
        .format("hudi") \
        .option("as.of.instant", "2023-09-20") \
        .load(base_path) \
        .show()


def update_data():
    updates = quickstart_utils.convertToStringList(dataGen.generateUpdates(10))
    df = spark.read.json(spark.sparkContext.parallelize(updates, 2))
    df.write \
        .format("hudi") \
        .mode("append") \
        .save(base_path)

    query_data()


def incremental_query():
    ordered_rows: list[Row] = spark.read \
        .format("hudi") \
        .load(base_path) \
        .select(col("_hoodie_commit_time").alias("commit_time")) \
        .orderBy(col("commit_time")) \
        .collect()

    commits: list[Any] = list(map(lambda row: row[0], ordered_rows))
    begin_time = commits[0]

    # print(commits)
    # print(begin_time)

    incremental_read_options = {
        'hoodie.datasource.query.type': 'incremental',
        'hoodie.datasource.read.begin.instanttime': begin_time,
    }

    trips_incremental_df = spark.read \
        .format("hudi") \
        .options(**incremental_read_options) \
        .load(base_path)

    trips_incremental_df.show()

    trips_incremental_df \
        .where(col("fare") > 20.0) \
        .select(col("_hoodie_commit_time"),
                col("fare"),
                col("begin_lon"),
                col("begin_lat"),
                col("ts")) \
        .show()


if __name__ == '__main__':
    # create_df()
    # write_data()
    # query_data()
    # time_travel_query()
    # update_data()
    incremental_query()