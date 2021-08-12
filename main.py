import findspark
import numpy as np
import pandas as pd

findspark.init()
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
import matplotlib.pyplot as plt

"""
TODO:

Qs for Afik's reception hour:
1. Do we load enough?
2. Do our insights mast be related to PRCP?
3. Can we hold a separate table for each insight?
4. Does our code should show the plots or its on another notebook?
5. Should we keep the data of our insights in the DB server, or can we show graph on the fly?
6. How can we overwrite TOO LONG time of waiting? cluster is not faster!
7. What prediction should we show? PRCP where, how when?
 
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
    """
    Spark initialization
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def StaticStream():
    """
    Initialized Spark, defines the data schema and reads from Kafka into a static DataFrame
    :return: Spark variables and kafka_df which contains the data
    """
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
        .option("subscribe", "EZ, US") \
        .option("startingOffsets", "earliest") \
        .load()
    kafka_value_df = kafka_raw_df.selectExpr("CAST(value AS STRING)")
    json_df = kafka_value_df.select(F.from_json(F.col("value"), schema=noaa_schema).alias('json'))
    # Flatten the nested object:
    kafka_df = json_df.select("json.*")
    # kafka_df.show()
    return os, spark, sc, kafka_df


def ReadDf(spark, tableName):
    """

    :param spark:
    :param tableName:
    :return:
    """
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


def readStreamData(spark, flag="false"):
    """

    :param spark:
    :param flag:
    :return:
    """
    os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1," \
                                        "com.microsoft.azure:spark-mssql-connector_2.12:1.1.0 pyspark-shell"
    # spark, sc = init_spark('RoiAndYarin')
    kafka_server = 'dds2020s-kafka.eastus.cloudapp.azure.com:9092'
    noaa_schema = StructType([StructField('StationId', StringType(), False),
                              StructField('Date', IntegerType(), False),
                              StructField('Variable', StringType(), False),
                              StructField('Value', IntegerType(), False),
                              StructField('M_Flag', StringType(), True),
                              StructField('Q_Flag', StringType(), True),
                              StructField('S_Flag', StringType(), True),
                              StructField('ObsTime', StringType(), True)])
    if (flag):
        streaming_input_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_server) \
            .option("subscribe", "EZ, US") \
            .option("startingOffsets", "earliest") \
            .load() \
            .limit(10000) \
            .selectExpr("CAST(value AS STRING)") \
            .select(F.from_json(F.col("value"), schema=noaa_schema).alias('json')) \
            .select("json.*") \
            .drop("M_Flag", "S_Flag", "ObsTime") \
            .filter(F.col("Q_Flag").isNull()) \
            .drop("Q_Flag") \
            .withColumn("fullDate", F.to_date(F.concat(F.col("Date")), "yyyyMMdd")) \
            .drop("Date")
    else:
        streaming_input_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_server) \
            .option("subscribe", "EZ, US") \
            .option("startingOffsets", "earliest") \
            .load() \
            .limit(100000) \
            .selectExpr("CAST(value AS STRING)") \
            .select(F.from_json(F.col("value"), schema=noaa_schema).alias('json')) \
            .select("json.*") \
            .drop("M_Flag", "S_Flag", "ObsTime") \
            .filter(F.col("Q_Flag").isNull()) \
            .drop("Q_Flag") \
            .withColumn("fullDate", F.to_date(F.concat(F.col("Date")), "yyyyMMdd")) \
            .groupBy(['Stationid', 'Year']).pivot('Variable').agg(F.avg("value")).orderBy("Year") \
            .drop("Date") \
            .show()

    query = streaming_input_df \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, epoch_id: transformChangePerContinent(df, "WeatherTestTable")) \
        .trigger(processingTime='60 seconds') \
        .start()
    query.awaitTermination()
    print("finished")


def writeToserver(df, tableName):
    """
    Writes the Spark Dataframe 'df' to the server under the name 'tableName', where 'tableName' is an existing table on
    the server
    :param df: Spark Dataframe
    :param tableName: The name of the table in the server that will contain the Dataframe 'df'
    :return: -
    """
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
    """

    :param df:
    :param GeoData: GeoData(FIPS, Continent, CountryName) table on the DB, used for extracting the country and continent
    :return:
    """
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
    """

    :param df:
    :return:
    """
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
    """

    :param df:
    :return:
    """
    pass


def make_first_graph(dict):
    """

    :param dict:
    :return:
    """
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
    """

    :param kafka_df:
    :return:
    """
    Dfsnow = kafka_df.select(['FIPS', 'Year', 'Variable', 'Value', 'Q_Flag'])
    Dfsnow = Dfsnow.filter("Variable ==SNOW").limit(300000)


def fixed_width_txtfile_converter(filename, colspecs, colnames, coltypes):
    """
    Creates a pd.dataframe from a given fixed-width txt file.
    :param filename: Name of the txt file in fixed-width format. File must be in the project directory
    :param colspecs: A list of 2-tuples with ints, representing the widths of the columns
    :param colnames: A list of strings representing column names for the df
    :param coltypes: A dictionary (keys are str, values are np types or str) representing column types for the df
    :return: a pd.dataframe of the fixed-width file
    """

    """
    About fixed-width format, from the project's guidelines:
    The file format is a fixed-width format. In fixed width format, each variable has a fixed width in characters.
    There is no delimiter.
    """

    # Start by converting the fixed-width file to CSVs:
    col_specs = colspecs
    col_names = colnames
    read_file = pd.read_fwf(f"{filename}.txt", colspecs=col_specs)
    read_file.to_csv(f"{filename}.csv", sep=';', header=col_names, index=False)

    # Break the single column
    col_types = coltypes
    read_file = pd.read_csv(f"{filename}.csv", delimiter=';', dtype=col_types)
    os.remove(f"{filename}.csv")
    # read_file.to_csv(f"{filename}.csv", sep=';', header=col_names, index=False)
    # print(read_file.head(10))
    # print(read_file.loc[67606])
    # print(read_file.loc[1004])
    # print(f"{filename} preview:")
    return read_file


def main():
    os, spark, sc, kafka_df = StaticStream()
    # spark, sc = init_spark('RoiAndYarin')  # Unnecessary, runs inside StaticSteams()
    # print(initiateServer())
    # readStreamData(spark)
    # GeoData = ReadDf(spark, tableName="GeoData")
    # dict1 = transformChangePerContinent(kafka_df, GeoData)
    # make_first_graph(dict1)
    # temporal_insight(kafka_df)

    # writeToserver(tDf, "AvgPRCPPerYeat")

    # The next 30 lines convert the txt files to tables on the server, do NOT run them again

    # # Creating DFs for the txt files and uploading them to the server
    # stations_colspecs = [(0, 11), (12, 20), (22, 30), (31, 37), (38, 40), (41, 71), (72, 75), (76, 79), (80, 85)]
    # stations_col_names = ['StationID', 'LATITUDE', 'LONGITUDE', 'ELEVATION', 'STATE', 'NAME', 'GSN_FLAG',
    #                       'HCN/CRN_FLAG',
    #                       'WMO_ID']
    # stations_col_types = {'StationID': str, 'LATITUDE': np.float64, 'LONGITUDE': np.float64, 'ELEVATION': np.float64,
    #                       'STATE': str, 'NAME': str, 'GSN_FLAG': str, 'HCN/CRN_FLAG': str, 'WMO_ID': str}
    #
    # stations_df = fixed_width_txtfile_converter("ghcnd-stations", stations_colspecs, stations_col_names,
    #                                             stations_col_types)  # pandas.DataFrame of ghcnd-stations
    #
    # inventory_colspecs = [(0, 11), (12, 20), (22, 30), (31, 35), (36, 40), (41, 45)]
    # inventory_col_names = ['StationID', 'LATITUDE', 'LONGITUDE', 'VARIABLE', 'FIRSTYEAR', 'LASTYEAR']
    # inventory_col_types = {'StationID': str, 'LATITUDE': np.float64, 'LONGITUDE': np.float64, 'VARIABLE': str,
    #                        'FIRSTYEAR': np.int64, 'LASTYEAR': np.int64}
    #
    # inventory_df = fixed_width_txtfile_converter("ghcnd-inventory", inventory_colspecs, inventory_col_names,
    #                                              inventory_col_types)  # pandas.DataFrame of ghcnd-inventory
    #
    # # Upload to the server
    # stations_df[['GSN_FLAG', 'WMO_ID', 'STATE', 'HCN/CRN_FLAG']] = stations_df[  # Fixes the createDataFrame() function
    #     ['GSN_FLAG', 'WMO_ID', 'STATE', 'HCN/CRN_FLAG']].astype(str)             # call in the next row (Throws an
    #                                                                              # exception for some reason)
    # stations_spark_df = spark.createDataFrame(stations_df)  # spark.DataFrame of ghcnd-stations
    # writeToserver(stations_spark_df, 'GHCND_Stations')
    #
    # inventory_spark_df = spark.createDataFrame(inventory_df)  # spark.DataFrame of ghcnd-inventory
    # writeToserver(inventory_spark_df, 'GHCNDInventory')  # Probably too big for writeToserver(), imported manually


if __name__ == '__main__':
    main()
