# import findspark

# findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import os
from pyspark.sql.types import *
import numpy as np
import pandas as pd
import time


# import matplotlib.pyplot as plt

def init_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


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
        .trigger(processingTime='120 seconds') \
        .foreachBatch(lambda df, epoch_id: transformation_func(df)) \
        .start() \
        .awaitTermination(int(7200))


def spatial_info_pre_processing(df):
    df_stations = read_df_from_sql_server('GHCND_Stations')
    df_stations = df_stations.select(['StationID', 'ELEVATION'])
    df = df.filter(F.col("Variable") == 'SNOW').withColumn("Month", F.month("fullDate"))
    df = df.filter(F.col("Month") == "1").withColumnRenamed('ELEVATION', 'Elevation')
    df = df.join(df_stations, on='StationID').drop('Variable', 'StationId').withColumnRenamed('Value', 'SNOW')
    df = df.groupBy(['Elevation']).agg(F.sum('SNOW').alias('Sum_Snow'), F.count('SNOW').alias('Count_Snow')).orderBy(F.col('Elevation').asc())
    write_to_server(df, 'Insight3_pre_Canada_Snow')


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


def main():
    amount = 5000000
    amount = 5000
    readStreamData('CA', amount, spatial_info_pre_processing)


if __name__ == '__main__':
    main()
