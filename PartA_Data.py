from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
# from pyspark import SparkFiles
import os

"""
ReadME:
Core variables: PRCP, SNOW, SNWD, TMAX, TMIN
Data gathered by Reading dynamic streaming

"""


def init_spark(app_name: str):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def streamData(country, amount, transformation_func, mode="dynamic"):
    if mode == "static":
        StaticStream(country)
    if mode == "dynamic":
        DynamicStreamData(country, amount, transformation_func)


def StaticStream(country):
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
        .option("subscribe", country) \
        .option("startingOffsets", "earliest") \
        .load()
    kafka_value_df = kafka_raw_df.selectExpr("CAST(value AS STRING)")
    json_df = kafka_value_df.select(F.from_json(F.col("value"), schema=noaa_schema).alias('json'))
    # Flatten the nested object:
    kafka_df = json_df.select("json.*")
    return os, spark, sc, kafka_df


def DynamicStreamData(country, amount, transformation_func):
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
        .awaitTermination(int(2 * 3600))  # 2 hours, tested ampirically to ensures getting over 100000000 rows


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


# -------------------Insight1---------------------------
def temporal_insight_pre(df):
    df = df.withColumn("Year", F.year("fullDate"))
    df = df.select(['Year', 'Variable', 'Value'])
    df = df.filter("Variable == 'TMAX' or Variable == 'TMIN'")
    df = df.groupBy(['Year']).pivot('Variable').agg(F.sum("value").alias('sum'),
                                                    F.count("value").alias('count')).orderBy("Year")
    writeToserver(df, "insight1_pre_EZ")


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


# -------------------Insight2---------------------------
def tempo_spatial_pre(df):
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
    continents = ["Europe", "Americas", "Asia"]
    for c in continents:
        ChangePerContinent[c] = data_joined.filter(F.col("continent") == c)
        writeToserver(ChangePerContinent[c], c + "Insight2")


def tempo_spatial_post_process():
    continent = {"Europe": read_df_from_sql_server("EuropeInsight2"),
                 "Americas": read_df_from_sql_server("AmericasInsight2"),
                 "Asia": read_df_from_sql_server("AsiaInsight2")}
    for key in continent.keys():
        continent[key] = continent[key].groupBy(F.col("year")).agg(F.sum(F.col("PRCP")).alias("PRCP"),
                                                                   F.sum(F.col("counter")).alias("counter"))
        continent[key] = continent[key].withColumn("avg_PRCP", F.col("PRCP") / F.col("counter"))
        continent[key] = continent[key].select(['year', 'avg_PRCP']).orderBy("year")
        continent[key] = continent[key].filter(F.col("year") > 1956).filter(F.col("year") < 1993)
        writeToserver(continent[key], key + "PostInsight2")


# -------------------Insight3---------------------------
def spatial_info_pre_processing(df):
    df_stations = read_df_from_sql_server('GHCND_Stations')
    df_stations = df_stations.select(['StationID', 'ELEVATION'])
    df = df.filter(F.col("Variable") == 'SNOW').withColumn("Month", F.month("fullDate"))
    df = df.filter(F.col("Month") == "1").withColumnRenamed('ELEVATION', 'Elevation')
    df = df.join(df_stations, on='StationID').drop('Variable', 'StationId').withColumnRenamed('Value', 'SNOW')
    df = df.groupBy(['Elevation']).agg(F.sum('SNOW').alias('Sum_Snow'), F.count('SNOW').alias('Count_Snow')).orderBy(
        F.col('Elevation').asc())
    writeToserver(df, 'Insight3_pre_Us_Snow')


def spatial_info_post_processing():
    df = read_df_from_sql_server('insight3_pre_Us_Snow')
    df = df.groupBy('Elevation').agg(F.sum(F.col('Sum_Snow')).alias('Sum_Snow'),
                                     F.sum(F.col('Count_Snow')).alias('Count_Snow'))
    df = df.withColumn('avg_Snow', F.col('Sum_Snow') /
                       F.col('Count_Snow')).drop(F.col('Sum_Snow')) \
        .drop(F.col('Count_Snow')).sort(F.col('Elevation')).filter(F.col("Elevation") >= 0).filter(
        F.col("Elevation") < 1850).filter(F.col("avg_Snow") < 82)
    writeToserver(df, 'insight3_post_USA_Snow')


def WriteAppendixData():
    spark = SparkSession.builder.appName('weather').getOrCreate()
    station_df = spark.read.text('ghcnd-stations.txt')
    station_df = station_df.select(station_df.value.substr(0, 11).alias('StationId'),
                                   station_df.value.substr(12, 9).cast("float").alias('latitude'),
                                   station_df.value.substr(22, 9).cast("float").alias('longitude'),
                                   station_df.value.substr(32, 7).cast("float").alias('longitude'))
    writeToserver(station_df, 'GHCND_Stations')
    # Geo Data is a table we created manualy to convert FIPS country name and continent
    # url = "https://raw.githubusercontent.com/RoiPapo/SparkWetherPredictor/main/GeoData.csv"


# ------------------Learning-Data----------------------
def mlib_data(df):
    df = df.filter("Variable == 'TMAX' or Variable == 'TMIN' or Variable == 'SNOW' ").withColumn("month",
                                                                                                 F.month("fullDate"))
    df = df.filter("month == '1' or month == '2'")
    df = df.withColumn("year", F.year("fullDate"))
    df = df.groupBy(['StationId', 'year', 'month']).pivot('Variable').agg(F.sum("value").alias('sum'),
                                                                          F.count("value").alias('count')).show()
    writeToserver(df, "mlib_data")


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
    writeToserver(df, 'mlib_data_post')


# -------------------Bonus-Data-------------------------
def pre_bonus_data(df):
    df = df.filter(
        "Variable == 'PRCP' or Variable == 'SNOW' or Variable == 'SNWD' or Variable = 'TMAX' or Variable == 'TMIN'"). \
        withColumn("month", F.month("fullDate"))
    df = df.filter("month == '1' or month == '2'")
    df = df.groupBy(['StationId', 'fullDate']).pivot('Variable').agg(F.sum("value").alias('sum'),
                                                                     F.count("value").alias('count'))
    writeToserver(df, "bonus_data")


def post_bonus_data():
    df = read_df_from_sql_server('bonus_data').withColumn('month', F.month(F.col('fullDate'))).drop(F.col('fullDate'))
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
    df = df.withColumn('avg_Snow', F.col('Snow_sum') / F.col('Snow_count')).drop(F.col('Snow_sum')) \
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
    writeToserver(df, 'bonus_post')


# ---------------------------------------------------


def main():
    WriteAppendixData()
    amount_per_batch = 5000000
    streamData("EZ", amount_per_batch, temporal_insight_pre, "dynamic")  # Data for Insight1
    temporal_insight_post()  # Data for Insight1

    streamData("EZ, US, CH, FR", amount_per_batch, tempo_spatial_pre, "dynamic")  # Data for Insight2
    tempo_spatial_post_process()  # Data for Insight2

    streamData('US', amount_per_batch, spatial_info_pre_processing, "dynamic")  # Data for Insight3
    spatial_info_post_processing()  # Data for Insight3

    streamData('CA', amount_per_batch, mlib_data, "dynamic")  # Data for learning
    ml_lib_post_processing()

    streamData('CA', amount_per_batch, pre_bonus_data, "dynamic")  # Data for Bonus
    post_bonus_data()


if __name__ == '__main__':
    main()
