from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

import os
import time

import numpy as np
import pandas as pd


def init_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def read_df_from_sql_server(tableName):
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1," \
                                        "com.microsoft.azure:spark-mssql-connector_2.12:1.1.0 pyspark-shell"

    kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'
    spark, sc = init_spark('RoiAndYarin')
    os.environ["SPARK_HOME"] = "/content/spark-3.1.2-bin-hadoop3.2"
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1," \
                                        "com.microsoft.azure:spark-mssql-connector_2.12:1.1.0 pyspark-shell"
    server_name = "jdbc:sqlserver://technionddscourse.database.windows.net:1433"
    database_name = "yarinbs"
    url = server_name + ";" + "databaseName=" + database_name + ";"
    spark = SparkSession.builder.getOrCreate()
    table_name = tableName
    username = "yarinbs"
    password = "Qwerty12!"  # Please specify password here
    format_str = "com.microsoft.sqlserver.jdbc.spark"
    format_str2 = "jdbc"
    format_str3 = "com.mysql.jdbc.Driver"
    jdbcDF = spark.read \
        .format(format_str2) \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password).load()
    return jdbcDF


def readStreamData(country, amount, transformation_func):
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1," \
                                        "com.microsoft.azure:spark-mssql-connector_2.12:1.1.0 pyspark-shell"
    spark, sc = init_spark('RoiAndYarin')
    kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'
    noaa_schema = StructType([StructField('StationId', StringType(), False),
                              StructField('Date', IntegerType(), False),
                              StructField('Variable', StringType(), False),
                              StructField('Value', IntegerType(), False),
                              StructField('M_Flag', StringType(), True),
                              StructField('Q_Flag', StringType(), True),
                              StructField('S_Flag', StringType(), True),
                              StructField('ObsTime', StringType(), True)])

    streaming_input_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", country) \
        .option("maxOffsetsPerTrigger", amount) \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(F.from_json(F.col("value"), schema=noaa_schema).alias('json')) \
        .select("json.*") \
        .drop("M_Flag", "S_Flag", "ObsTime", 'Q_flag') \
        .withColumn("fullDate", F.to_date(F.concat(F.col("Date")), "yyyyMMdd")) \
        .drop("Date") \
        .filter(F.col("Q_Flag").isNull()) \
        .drop("Q_Flag")

    query = streaming_input_df \
        .writeStream \
        .trigger(processingTime='140 seconds') \
        .foreachBatch(lambda df, epoch_id: transformation_func(df)) \
        .start() \
        .awaitTermination(int(7200))


def bonus_data(df):
    df = df.filter(
        "Variable == 'PRCP' or Variable == 'SNOW' or Variable == 'SNWD' or Variable = 'TMAX' or Variable == 'TMIN'"). \
        withColumn("month", F.month("fullDate"))
    df = df.filter("month == '1' or month == '2'")
    df = df.groupBy(['StationId', 'fullDate']).pivot('Variable').agg(F.sum("value").alias('sum'),
                                                                     F.count("value").alias('count'))
    write_to_server(df, "bonus_data")


def write_to_server(df, tableName):
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1," \
                                        "com.microsoft.azure:spark-mssql-connector_2.12:1.1.0 pyspark-shell"
    server_name = "jdbc:sqlserver://technionddscourse.database.windows.net"
    database_name = "yarinbs"
    url = server_name + ";" + "databaseName=" + database_name + ";"

    table_name = tableName
    username = "yarinbs"
    password = "Qwerty12!"  # Please specify password here
    format_str = "com.microsoft.sqlserver.jdbc.spark"
    format_str2 = "jdbc"

    try:
        df.write \
            .format(format_str2) \
            .mode("append") \
            .option("url", url) \
            .option("dbtable", table_name) \
            .option("user", username) \
            .option("password", password) \
            .save()
    except ValueError as error:
        print("Connector write failed", error)


def post_pre_proccessing():
    df = read_df_from_sql_server('bonus').withColumn('month', F.month(F.col('fullDate'))).drop(F.col('fullDate'))
    df = df.groupBy(['StationId', 'month']).agg(F.sum(F.col('Snow_sum')).alias('Snow_sum'),
                                                   F.sum(F.col('Snow_count')).alias('Snow_count'),
                                                   F.sum(F.col('PRCP_sum')).alias('PRCP_sum'),
                                                   F.sum(F.col('PRCP_count')).alias('PRCP_count'),
                                                   F.sum(F.col('SNWD_sum')).alias('SNWD_sum'),
                                                   F.sum(F.col('SNWD_count')).alias('SNWD_count'),
                                                   F.sum(F.col('TMAX_sum')).alias('TMAX_sum'),
                                                   F.sum(F.col('TMAX_count')).alias('TMAX_count'),
                                                   F.sum(F.col('TMIN_sum')).alias('TMIN_sum'),
                                                   F.sum(F.col('TMIN_count')).alias('TMIN_count'))
    df = df.withColumn('avg_Snow', F.col('Snow_sum') /F.col('Snow_count')).drop(F.col('Snow_sum')) \
        .drop(F.col('Snow_count'))
    df = df.withColumn('PRCP_avg', F.col('PRCP_sum') / F.col('PRCP_count')).drop(F.col('PRCP_sum')) \
        .drop(F.col('PRCP_count'))
    df = df.withColumn('SNWD_AVG', F.col('SNWD_sum') / F.col('SNWD_count')).drop(F.col('SNWD_sum')) \
        .drop(F.col('SNWD_count'))
    df = df.withColumn('TMAX_avg', F.col('TMAX_sum') / F.col('TMAX_count')).drop(F.col('TMAX_sum')) \
        .drop(F.col('TMAX_count'))
    df = df.withColumn('TMIN_avg', F.col('TMIN_sum') / F.col('TMIN_count')).drop(F.col('TMIN_sum')) \
        .drop(F.col('TMIN_count'))
    df = df.drop(F.col('month'))
    write_to_server(df, 'bonus_post')


def main():
    post_pre_proccessing()


if __name__ == '__main__':
    main()
