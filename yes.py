#import findspark

#findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import os
from pyspark.sql.types import *
#import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import time

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


# def StaticStream():
#     start_time = time.time()
#     os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1," \
#                                         "com.microsoft.azure:spark-mssql-connector_2.12:1.1.0 pyspark-shell"
#     spark, sc = init_spark('RoiAndYarin')
#     kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'
#     noaa_schema = StructType([StructField('StationId', StringType(), False),
#                               StructField('Date', IntegerType(), False),
#                               StructField('Variable', StringType(), False),
#                               StructField('Value', IntegerType(), False),
#                               StructField('M_Flag', StringType(), True),
#                               StructField('Q_Flag', StringType(), True),
#                               StructField('S_Flag', StringType(), True),
#                               StructField('ObsTime', StringType(), True)])
#     kafka_raw_df = spark \
#         .read \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", kafka_server) \
#         .option("subscribe", "CH") \
#         .option("startingOffsets", "earliest") \
#         .load() \
#         .limit(5_000_000_00)
#     kafka_value_df = kafka_raw_df.selectExpr("CAST(value AS STRING)")
#     json_df = kafka_value_df.select(F.from_json(F.col("value"), schema=noaa_schema).alias('json'))
#     # Flatten the nested object:
#     kafka_df = json_df.select("json.*")
#     print((kafka_df.count(), len(kafka_df.columns)))
#     print(f"Took {time.time() - start_time} seconds")
#     kafka_df.repartition(1).write.format('com.databricks.spark.csv').save("data saved/china_df.csv", header='true')
#     return os, spark, sc, kafka_df



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


def readStreamData(country, var, amount):
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
        .option("startingOffsets", "earliest") \
        .load() \
        .limit(amount) \
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
        .outputMode("append") \
        .foreachBatch(lambda df, epoch_id: spactioal_info(df, country, var)) \
        .trigger(processingTime='60 seconds') \
        .start()
    query.awaitTermination()


def spactioal_info(df, country, var):
    df_stations = ReadDf2('GHCND_Stations')
    df_stations = df_stations.select(['StationID', 'ELEVATION'])
    df = df.filter(F.col("Variable") == var).withColumn("Month", F.month(
        "fullDate"))  # .withColumn("Year", F.year("fullDate"))
    df = df.filter(F.col("Month") == "1")  # .filter(F.col("Year") > 1950)
    df_joineds = df.join(df_stations, on='StationID').drop('Variable', 'StationId').withColumnRenamed('Value', var)
    df_joineds = df_joineds.groupBy(['ELEVATION']).agg(F.avg(var).alias(var)).orderBy(
        F.col('ELEVATION').asc())  ###orderBy("ELEVATION",'Year', 'Month').show()
    pandush = df_joineds.toPandas()
    import matplotlib.pyplot as plt
    plt.plot(pandush['ELEVATION'], pandush[var], label="WOW")
    plt.xlabel('ELEVATION')
    plt.ylabel('SNOW-AvgMinTemp')
    plt.title('Annualstaff')
    plt.legend()
    plt.show()

    #writeToserver(df_joineds, country+'_'+var)


def staiotns():
    with open('ghcnd-stations.txt') as f:
        lines = f.readlines()
    useful_stations = []
    for line in lines:
        useful_stations.append([line[:12].replace(' ', ''), float(line[31:37].replace(' ', ''))])
    # useful_stations = np.array(useful_stations)
    columns_lst = ['StationId', 'altitude']
    df_stations = pd.DataFrame(data=useful_stations, columns=columns_lst)
    return useful_stations


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


def transformChangePerContinent(df, GeoData="null"):
    # Df 'Date' column transformation
    # df = df.withColumn("fullDate", F.to_date(F.concat(F.col("Date")), "yyyyMMdd"))
    df = df.withColumn("Year", F.year("fullDate"))
    df = df.withColumn('FIPS', F.substring(df['StationId'], 0, 2))
    df = df.select(['FIPS', 'Year', 'Variable', 'Value', 'Q_Flag'])
    df = df.filter(df.Q_Flag.isNull())  # Remove rows where Q_Flag is not Null
    df = df.filter(df.Variable == "PRCP").limit(300000)
    data_joined = df.join(GeoData, on=['FIPS'])
    data_joined = data_joined.groupBy(['continent', 'year']).agg(F.avg('Value').alias("PRCP")).orderBy("year")
    data_joined = data_joined.repartitionByRange(4, F.col("continent"))
    ChangePerContinent = {}
    # ChangePerContinent["Europe"] = data_joined.filter(data_joined.continent == "Europe").toPandas()
    # ChangePerContinent["Asia"] = data_joined.filter(data_joined.continent == "Asia").toPandas()
    # ChangePerContinent['Africa'] = data_joined.filter(data_joined.continent == "Africa").toPandas()
    # ChangePerContinent['Oceania'] = data_joined.filter(data_joined.continent == "Oceania").toPandas()
    # ChangePerContinent['Americas'] = data_joined.filter(data_joined.continent == "Americas").toPandas()
    continents = data_joined.select('continent').distinct().tolist()
    for c in continents:
        ChangePerContinent[c] = data_joined.filter(data_joined.continent == c)
        writeToserver(ChangePerContinent[c], c + "PRCPPerYear")
    # # df = df.groupBy(['StationId', 'Year']).pivot('Variable').avg('Value')
    # df = df.filter(df.PRCP.isNotNull())  # Remove rows where Q_Flag is not Null
    # dfAVGperYear = df.withColumn("Year", F.year("fullDate"))
    # dfAVGperYear = dfAVGperYear.groupBy("year").agg(F.avg("PRCP").alias("AvgPrcp"))
    # data_joined = df.join(GeoData, on=['FIPS'])
    # data_joined.show(100)
    # return ChangePerContinent


def temporal_insight(df):
    print(df.count())
    # Temporal insight - global max/min temperature trends

    # df1 = df.select(['Year', 'Variable', 'Value']).where("Variable = PRCP")
    # df1 = df1.groupby(['Year']).agg(F.avg('Value').alias("Yearly Average Precipitation")).orderBy("Year")
    # AvgPRCPdf = df1.select(['Year', 'Value']).toPandas()
    # plt.plot(AvgPRCPdf, label="Average")

    # df = df.withColumn("fullDate", F.to_date(F.concat(F.col("Date")), "yyyyMMdd"))
    df = df.withColumn("Year", F.year("fullDate"))
    df = df.select(['Year', 'Variable', 'Value'])
    df = df.filter("Variable == 'TMAX' or Variable == 'TMIN'").limit(300000)
    df = df.groupBy(['Year']).pivot('Variable').agg(F.avg("value")).orderBy("Year")
    # df2 = df2.filter(df.TMIN.isNotNull() & df.TMAX.isNotNull()).limit(3000)
    df = df.withColumn('Gap', F.col("TMAX") - F.col("TMIN"))
    # df2 = df2.groupby(['Year']).agg(F.max('Gap'))
    savetoDB = df.select(['Year', 'Gap'])
    writeToserver(savetoDB, "world_year_Temp_diff")
    MaxPRCPdf = savetoDB.toPandas()
    plt.plot(MaxPRCPdf['Year'], MaxPRCPdf['Gap'], label="Maximum")

    # df3 = df.select(['Year', 'Variable', 'Value']).filter(df.Variable == "TMIN").limit(5000)
    # df3 = df3.groupby(['Year']).agg(F.min('Value').alias("min")).orderBy("Year")
    # MinPRCPdf = df3.select(['Year', 'min']).toPandas()
    # plt.plot(MinPRCPdf['Year'], MinPRCPdf['min'], label="Minimum")

    plt.xlabel('Years')
    plt.ylabel('AvgMaxTemp-AvgMinTemp')
    plt.title('Annual Global Maximum-Minimum Temperature Trends')
    plt.legend()
    plt.show()


def spatial_insight(df):
    pass


def make_first_graph(dict):
    # x = np.arange(-4,2*np.pi, 0.3)
    # y = 2*np.sin(x)
    # y2 = 3*np.cos(x)
    for key in dict.keys():
        x = dict[key]["year"]
        y = dict[key]["PRCP"]
        plt.plot(dict[key]["year"], dict[key]["PRCP"], '-gD')
        for xitem, yitem in np.nditer([x, y]):
            etiqueta = "{:.1f}".format(xitem)
            # plt.annotate(etiqueta, (xitem, yitem), textcoords="offset points", xytext=(0, 10), ha="center")
    plt.grid(True)
    plt.show()


def snowTransform(kafka_df):
    Dfsnow = kafka_df.select(['FIPS', 'Year', 'Variable', 'Value', 'Q_Flag'])
    Dfsnow = Dfsnow.filter("Variable ==SNOW").limit(300000)


def ReadDf2(tableName):
    import os
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
    amount = 10000
    country_lst = ['US', 'AS', 'CA', 'BR', 'MX', 'IN', 'SW']
    var_lst = ['SNOW', 'PRCP']
    country_lst = ['AS']
    var_lst = ['SNOW']
    for country in country_lst:
        for var in var_lst:
            readStreamData(country, var, amount)


if __name__ == '__main__':
    main()
