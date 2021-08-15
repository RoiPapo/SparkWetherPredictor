# import findspark
import numpy as np
import pandas as pd

# findspark.init()
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
# from pyspark import SparkFiles
import os

import matplotlib.pyplot as plt

"""
TODO:

 ASK:
 1. do we load enoght ?
 2. do our maskanot mast be realated to PRCP
 3. can we hold seperate table for each maskena ?
 4. does our code should show the plots or its on another notebook
 5. should we keep the data of our maskanot in the DB server ? or can we show graph on the fly?
 6. how can we overrite TOO LONG time of waiting ? cluster is not faster!
 7. what prediction shhould we show ? prcp where ? how ? and when ?
 
1. Data Analysis:
    a. Derive a temporal-based insight:
        * 
    b. Derive a spatial-based insight:
        * the country with highest precipitation
    c. Derive a spatio-temporal-based insight:
        * for each continent, plot the yearly average precipitation

Core variables: PRCP, SNOW, SNWD, TMAX, TMIN
Irrelevant variables: AWDR, AWND, DAWM, FMTM, FRGB, FRGT, FRTH, GAHT, MDWM, PGTM, THIC, WDF1, WDF2, WDF5, WDFG, WDFI,
WDFM, WDMV, WSF1, WSF2, WSF5, WSFG, WSFI, WSFM, 

Precipitation can be predicted by observing past precipitation records, max/min temperatures, snowfall, cloudiness.
Wind records may be irrelevant.

"""


def init_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def StaticStream():
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
    kafka_raw_df = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_server) \
        .option("subscribe", "CH") \
        .option("startingOffsets", "earliest") \
        .load()
    kafka_value_df = kafka_raw_df.selectExpr("CAST(value AS STRING)")
    json_df = kafka_value_df.select(F.from_json(F.col("value"), schema=noaa_schema).alias('json'))
    # Flatten the nested object:
    kafka_df = json_df.select("json.*")
    return os, spark, sc, kafka_df


def ReadDf(spark, tableName):
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
    jdbcDF = spark.read \
        .format("com.microsoft.sqlserver.jdbc.spark") \
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
        .trigger(processingTime='120 seconds') \
        .foreachBatch(lambda df, epoch_id: transformation_func(df)) \
        .start() \
        .awaitTermination(int(2 * 3600))


def writeToserver(df, tableName):
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


def tempo_spatial_pre(df, GeoData="null"):
    # Df 'Date' column transformation
    # df = df.withColumn("fullDate", F.to_date(F.concat(F.col("Date")), "yyyyMMdd"))
    GeoData = read_df_from_sql_server("GeoData")
    df = df.withColumn("Year", F.year("fullDate"))
    df = df.withColumn('FIPS', F.substring(df['StationId'], 0, 2))
    df = df.select(['FIPS', 'Year', 'Variable', 'Value'])
    df = df.filter(df.Variable == "PRCP")
    data_joined = df.join(GeoData, on=['FIPS'])
    data_joined = data_joined.groupBy(['continent', 'year']).agg(F.sum('Value') \
                                                                 .alias("PRCP"),
                                                                 F.count('value').alias("counter")).orderBy(
        "year")
    data_joined = data_joined.repartitionByRange(4, F.col("continent"))
    ChangePerContinent = {}
    # ChangePerContinent["Europe"] = data_joined.filter(data_joined.continent == "Europe").toPandas()
    # ChangePerContinent["Asia"] = data_joined.filter(data_joined.continent == "Asia").toPandas()
    # ChangePerContinent['Africa'] = data_joined.filter(data_joined.continent == "Africa").toPandas()
    # ChangePerContinent['Oceania'] = data_joined.filter(data_joined.continent == "Oceania").toPandas()
    # ChangePerContinent['Americas'] = data_joined.filter(data_joined.continent == "Americas").toPandas()
    continents = ["Europe", "Americas", "Asia"]
    for c in continents:
        ChangePerContinent[c] = data_joined.filter(F.col("continent") == c)
        # ChangePerContinent[c].show()
        writeToserver(ChangePerContinent[c], c + "Insight2")
    # # df = df.groupBy(['StationId', 'Year']).pivot('Variable').avg('Value')
    # df = df.filter(df.PRCP.isNotNull())  # Remove rows where Q_Flag is not Null
    # dfAVGperYear = df.withColumn("Year", F.year("fullDate"))
    # dfAVGperYear = dfAVGperYear.groupBy("year").agg(F.avg("PRCP").alias("AvgPrcp"))
    # data_joined = df.join(GeoData, on=['FIPS'])
    # data_joined.show(100)
    # return ChangePerContinent


def temporal_insight_pre(df):  # insight2
    # Temporal insight - global max/min temperature trends

    # df1 = df.select(['Year', 'Variable', 'Value']).where("Variable = PRCP")
    # df1 = df1.groupby(['Year']).agg(F.avg('Value').alias("Yearly Average Precipitation")).orderBy("Year")
    # AvgPRCPdf = df1.select(['Year', 'Value']).toPandas()
    # plt.plot(AvgPRCPdf, label="Average")

    # df = df.withColumn("fullDate", F.to_date(F.concat(F.col("Date")), "yyyyMMdd"))
    df = df.withColumn("Year", F.year("fullDate"))
    df = df.select(['Year', 'Variable', 'Value'])
    df = df.filter("Variable == 'TMAX' or Variable == 'TMIN'")
    df = df.groupBy(['Year']).pivot('Variable').agg(F.sum("value").alias('sum'),
                                                    F.count("value").alias('count')).orderBy("Year")
    writeToserver(df, "insight1_pre_EZ")


def tempo_spatial_post_process():  # insight2
    continent = {}
    continent["Europe"] = read_df_from_sql_server("EuropeInsight2")
    continent["Americas"] = read_df_from_sql_server("AmericasInsight2")
    continent["Asia"] = read_df_from_sql_server("AsiaInsight2")
    for key in continent.keys():
        continent[key] = continent[key].groupBy(F.col("year")).agg(F.sum(F.col("PRCP")).alias("PRCP"),
                                                                   F.sum(F.col("counter")).alias("counter"))
        continent[key] = continent[key].withColumn("avg_PRCP", F.col("PRCP") / F.col("counter"))
        continent[key] = continent[key].select(['year', 'avg_PRCP']).orderBy("year")
        continent[key]= continent[key].filter(F.col("year")>1956).filter(F.col("year")<1993)
        writeToserver(continent[key], key + "PostInsight2")


##### df = df.withColumn('Gap', F.col("TMAX") - F.col("TMIN"))
# df2 = df2.groupby(['Year']).agg(F.max('Gap'))
# savetoDB = df.select(['Year', 'Gap'])
# writeToserver(savetoDB, "insight1")
# MaxPRCPdf = savetoDB.toPandas()
# plt.plot(MaxPRCPdf['Year'], MaxPRCPdf['Gap'], label="Maximum")

# df3 = df.select(['Year', 'Variable', 'Value']).filter(df.Variable == "TMIN").limit(5000)
# df3 = df3.groupby(['Year']).agg(F.min('Value').alias("min")).orderBy("Year")
# MinPRCPdf = df3.select(['Year', 'min']).toPandas()
# plt.plot(MinPRCPdf['Year'], MinPRCPdf['min'], label="Minimum")

# plt.xlabel('Years')
# plt.ylabel('AvgMaxTemp-AvgMinTemp')
# plt.title('Annual Global Maximum-Minimum Temperature Trends')
# plt.legend()
# plt.show()


def temporal_insight_post():
    df = read_df_from_sql_server("insight1_pre_EZ")
    df = df.groupBy(F.col("year")).agg(F.sum(F.col("TMAX_sum")).alias("TMAX"), F.sum(F.col("TMIN_sum")).alias("TMIN"),
                                       F.sum(F.col("TMAX_count")).alias("s_TMAX_count"),
                                       F.sum(F.col("TMIN_count")).alias("s_TMIN_count"))
    df = df.withColumn("avg_TMAX", F.col("TMAX") / F.col("s_TMAX_count"))
    df = df.withColumn("avg_TMIN", F.col("TMIN") / F.col("s_TMIN_count"))
    df = df.withColumn('Gap', F.col("avg_TMAX") - F.col("avg_TMIN"))
    df = df.select(['year', 'Gap']).orderBy("year")
    writeToserver(df, "insight1_post_EZ")


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


#
# def spatial_insight_post_process():
#     df = read_df_from_sql_server('CA_SNOW')
#     df = df.withColumn('sum_snow', F.col('SNOW') * F.col('SNOW_count')).drop('SNOW')
#     df = df.groupBy('elevation').agg(F.sum('sum_snow').alias('sum_snow'), F.sum('SNOW_count').alias('SNOW_count'))
#     df = df.withColumn('avg_snow', F.col('sum_snow') / F.col('SNOW_count')).drop('SNOW_count').drop('sum_snow')
#     pandush = df.toPandas().sort_values(by=['elevation'])
#     plt.plot(pandush['elevation'], pandush['avg_snow'], label="snow")
#     plt.xlabel('elevation')
#     plt.ylabel('avg_snow-AvgMinTemp')
#     plt.title('Annual Global Maximum-Minimum Temperature Trends')
#     plt.legend()
#     plt.show()


def main():
    # os, spark, sc, kafka_df = StaticStream()
    # spark, sc = init_spark('RoiAndYarin')
    # url = "https://raw.githubusercontent.com/RoiPapo/SparkWetherPredictor/main/GeoData.csv"
    # spark.sparkContext.addFile(url)
    # spark.read.csv(SparkFiles.get("GeoData.csv"), header=True).show()
    # print(initiateServer())
    amount = 5000000
    # readStreamData("EZ, US, CH, FR", amount, tempo_spatial_pre)
    # tempo_spatial_post_process()
    continent = {}
    continent["Americas"] = read_df_from_sql_server("AmericasPostInsight2").toPandas().sort_values(by=['year'])
    continent["Europe"] = read_df_from_sql_server("EuropePostInsight2").toPandas().sort_values(by=['year'])
    continent["Asia"] = read_df_from_sql_server("AsiaPostInsight2").toPandas().sort_values(by=['year'])
    # colors = ['-g', '-r', '-c']
    i = 0
    for key in continent.keys():
        x = continent[key]["year"]
        y = continent[key]["avg_PRCP"]
        plt.bar(x, y, label=key)
        i = i + 1

    plt.xlabel('Years')
    plt.ylabel('PRCP')
    plt.title('avg PRCP by continent')
    plt.legend()
    plt.grid(True)
    plt.show()



    # GeoData = ReadDf(spark, tableName="GeoData")
    # dict1 = transformChangePerContinent(kafka_df, GeoData)
    # make_first_graph(dict1)
    # temporal_insight(kafka_df)
    # spatial_insight_post_process()
    # writeToserver(tDf, "AvgPRCPPerYeat")


if __name__ == '__main__':
    main()
