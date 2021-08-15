from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml.regression import LinearRegression
import os
from pyspark.sql.types import *
import numpy as np
import pandas as pd
import time
from pyspark.ml.linalg import Vector
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import udf
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler


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


def ml_lib_post_processing():
    df = read_df_from_sql_server('mlib_data').filter('year > 1970')
    df = df.groupBy(['StationID', 'year', 'month']).agg(F.sum(F.col('SNOW_sum')).alias('Snow_sum'),
                                                        F.sum(F.col('SNOW_count')).alias('Snow_count'),
                                                        F.sum(F.col('TMAX_sum')).alias('TMAX_sum'),
                                                        F.sum(F.col('TMAX_count')).alias('TMAX_count'),
                                                        F.sum(F.col('TMIN_sum')).alias('TMIN_sum'),
                                                        F.sum(F.col('TMIN_count')).alias('TMIN_count'),
                                                        F.sum(F.col('PRCP_sum')).alias('PRCP_sum'),
                                                        F.sum(F.col('PRCP_count')).alias('PRCP_count'))
    df = df.withColumn('Snow_avg', F.col('Snow_sum') / F.col('Snow_count')) \
        .withColumn('TMAX_avg', F.col('TMAX_sum') / F.col('TMAX_count')) \
        .withColumn('TMIN_avg', F.col('TMIN_sum') / F.col('TMIN_count')) \
        .withColumn('PRCP_avg', F.col('PRCP_sum') / F.col('PRCP_count'))
    df = df.drop('Snow_sum').drop('Snow_count').drop('TMAX_sum').drop('TMAX_count').drop('TMIN_sum').drop('TMIN_count') \
        .drop('PRCP_sum').drop('PRCP_count')
    write_to_server(df, 'mlib_data_post')


def L_reg(df):
    spark = SparkSession.builder.appName('lin_reg').getOrCreate()
    assembler = VectorAssembler(inputCols=['Snow_avg', 'TMAX_avg', 'TMIN_avg'], outputCol='features')
    output = assembler.transform(df)
    final_model_data = output.select('features', 'PRCP_avg')
    train_data, test_data = final_model_data.randomSplit([0.8, 0.2])

    lr = LinearRegression(labelCol='PRCP_avg')
    model = lr.fit(train_data)
    res = model.evaluate(test_data)
    # unlabeled_test_data = test_data.select('features')
    # predictions = model.transform(unlabeled_test_data)
    print(f'MSE: {res.rootMeanSquaredError}')


def pre_prossessing_for_Linear_Regrresion():
    df = read_df_from_sql_server('mlib_data_post')
    # print(df.count(), len(df.columns))
    df = df.na.drop()
    # print(df.count(), len(df.columns))
    # pandas_df = df.toPandas()
    # df = df.na.fill(value=-99.9, subset=["PRCP_avg"])
    # df = df.filter(F.col('PRCP_avg') != -99.9)
    # Snow_avg = pandas_df['Snow_avg'].median()
    # TMAX_avg = pandas_df['TMAX_avg'].median()
    # TMIN_avg = pandas_df['TMIN_avg'].median()
    # PRCP_avg = pandas_df['PRCP_avg'].median()
    # df = df.na.fill(value=Snow_avg, subset=["Snow_avg"])
    # df = df.na.fill(value=TMAX_avg, subset=["TMAX_avg"])
    # df = df.na.fill(value=TMIN_avg, subset=["TMIN_avg"])
    # df = df.na.fill(value=PRCP_avg, subset=["PRCP_avg"])
    # df = df.select(['Snow_avg', 'TMAX_avg', 'TMIN_avg', 'PRCP_avg'])
    return df


def clustering():
    df = read_df_from_sql_server('StationsForClustering')
    features = ['LATITUDE', 'LONGITUDE', 'ELEVATION']
    feature_vector = VectorAssembler(inputCols=features, outputCol='features')
    final_data = feature_vector.transform(df).select('StationId', 'features')
    scalar = StandardScaler(inputCol='features',outputCol='zfeatures',withStd=True, withMean=False)
    scalar_mdoel = scalar.fit(final_data)
    cluster_inputuda = scalar_mdoel.transform(final_data)

    kmeans = KMeans(featuresCol='features', k=10)
    model = kmeans.fit(input_for_kmeans)
    centers = model.clusterCenters()
    print(model.summary)
    print()


def main():
    clustering()


if __name__ == '__main__':
    main()
