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


# def fixed_width_txtfile_converter(filename, colspecs, colnames, coltypes):
#     """
#     Creates a pd.dataframe from a given fixed-width txt file.
#     :param filename: Name of the txt file in fixed-width format. File must be in the project directory
#     :param colspecs: A list of 2-tuples with ints, representing the widths of the columns
#     :param colnames: A list of strings representing column names for the df
#     :param coltypes: A dictionary (keys are str, values are np types or str) representing column types for the df
#     :return: a pd.dataframe of the fixed-width file
#     """
#
#     """
#     About fixed-width format, from the project's guidelines:
#     The file format is a fixed-width format. In fixed width format, each variable has a fixed width in characters.
#     There is no delimiter.
#     """
#
#     # Start by converting the fixed-width file to CSVs:
#     col_specs = colspecs
#     col_names = colnames
#     read_file = pd.read_fwf(f"{filename}.txt", colspecs=col_specs)
#     read_file.to_csv(f"{filename}.csv", sep=';', header=col_names, index=False)
#
#     # Break the single column
#     col_types = coltypes
#     read_file = pd.read_csv(f"{filename}.csv", delimiter=';', dtype=col_types)
#     os.remove(f"{filename}.csv")
#     # read_file.to_csv(f"{filename}.csv", sep=';', header=col_names, index=False)
#     # print(read_file.head(10))
#     # print(read_file.loc[67606])
#     # print(read_file.loc[1004])
#     # print(f"{filename} preview:")
#     return read_file

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
        .foreachBatch(lambda df, epoch_id: transformation_func(df, country)) \
        .start() \
        .awaitTermination(int(2*3600))


def spatial_info(df, country, var):
    df_stations = read_df_from_sql_server('GHCND_Stations')
    df_stations = df_stations.select(['StationID', 'ELEVATION'])
    df = df.filter(F.col("Variable") == var).withColumn("Month",
                                                        F.month("fullDate"))  # .withColumn("Year", F.year("fullDate"))
    df = df.filter(F.col("Month") == "1")  # .filter(F.col("Year") > 1950)
    df = df.join(df_stations, on='StationID').drop('Variable', 'StationId').withColumnRenamed('Value', var)
    df = df.groupBy(['ELEVATION']).agg(F.avg(var).alias(var)).orderBy(
        F.col('ELEVATION').asc())  ###orderBy("ELEVATION",'Year', 'Month').show()
    write_to_server(df, country + '_' + var)


def us_precipitation(df, country, var, epoch_id):
    # print(f"Entering Country {country} with variable {var} in epoch number {epoch_id}")
    df_stations = read_df_from_sql_server('GHCND_Stations')
    df_stations = df_stations.select(['StationID', 'ELEVATION'])
    df = df.filter(F.col("Variable") == var).withColumn("Month", F.month("fullDate"))
    # df = df.filter((F.col("Month") == "12") | (F.col("Month") == "1") | (F.col("Month") == "2") | (F.col("Month") == "3"))
    df = df.join(df_stations, on='StationID').drop('Variable', 'StationId').withColumnRenamed('Value', var).filter(
        F.col('ELEVATION') != -999.9)
    df = df.drop('fullDate', 'Month')
    df = df.groupBy(['ELEVATION']).agg(F.avg(var).alias(var), F.count(var).alias(var + '_count')).orderBy(
        F.col('ELEVATION').asc())
    # df.show()
    write_to_server(df, country + '_' + var)


def stations():
    with open('ghcnd-stations.txt') as f:
        lines = f.readlines()
    useful_stations = []
    for line in lines:
        useful_stations.append([line[:12].replace(' ', ''), float(line[31:37].replace(' ', ''))])
    columns_lst = ['StationId', 'altitude']
    df_stations = pd.DataFrame(data=useful_stations, columns=columns_lst)
    return df_stations


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
        write_to_server(ChangePerContinent[c], c + "PRCPPerYear")
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
    write_to_server(savetoDB, "world_year_Temp_diff")
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


def plot_insight(df, graph_name, x_label, y_label):
    df = df.toPandas()
    plt.scatter(df['elevation'], df['SNOW'], label="WOW")
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(graph_name)
    plt.legend()
    plt.show()


def txt_to_server(spark):
    # The next 30 lines convert the txt files to tables on the server, do NOT run them again

    # Creating DFs for the txt files and uploading them to the server
    stations_colspecs = [(0, 11), (12, 20), (22, 30), (31, 37), (38, 40), (41, 71), (72, 75), (76, 79), (80, 85)]
    stations_col_names = ['StationID', 'LATITUDE', 'LONGITUDE', 'ELEVATION', 'STATE', 'NAME', 'GSN_FLAG',
                          'HCN/CRN_FLAG',
                          'WMO_ID']
    stations_col_types = {'StationID': str, 'LATITUDE': np.float64, 'LONGITUDE': np.float64, 'ELEVATION': np.float64,
                          'STATE': str, 'NAME': str, 'GSN_FLAG': str, 'HCN/CRN_FLAG': str, 'WMO_ID': str}

    stations_df = fixed_width_txtfile_converter("ghcnd-stations", stations_colspecs, stations_col_names,
                                                stations_col_types)  # pandas.DataFrame of ghcnd-stations

    inventory_colspecs = [(0, 11), (12, 20), (22, 30), (31, 35), (36, 40), (41, 45)]
    inventory_col_names = ['StationID', 'LATITUDE', 'LONGITUDE', 'VARIABLE', 'FIRSTYEAR', 'LASTYEAR']
    inventory_col_types = {'StationID': str, 'LATITUDE': np.float64, 'LONGITUDE': np.float64, 'VARIABLE': str,
                           'FIRSTYEAR': np.int64, 'LASTYEAR': np.int64}

    inventory_df = fixed_width_txtfile_converter("ghcnd-inventory", inventory_colspecs, inventory_col_names,
                                                 inventory_col_types)  # pandas.DataFrame of ghcnd-inventory

    # Upload to the server
    stations_df[['GSN_FLAG', 'WMO_ID', 'STATE', 'HCN/CRN_FLAG']] = stations_df[  # Fixes the createDataFrame() function
        ['GSN_FLAG', 'WMO_ID', 'STATE', 'HCN/CRN_FLAG']].astype(str)  # call in the next row (Throws an
    # exception for some reason)
    stations_spark_df = spark.createDataFrame(stations_df)  # spark.DataFrame of ghcnd-stations
    write_to_server(stations_spark_df, 'GHCND_Stations')

    inventory_spark_df = spark.createDataFrame(inventory_df)  # spark.DataFrame of ghcnd-inventory
    write_to_server(inventory_spark_df, 'GHCNDInventory')  # Probably too big for writeToserver(), imported manually


def fetch_data_from_kafka():
    initial_time = time.time()
    # amount = 150000000
    amount = 10000
    country_lst = ['US', 'AS', 'CA', 'BR', 'MX', 'IN', 'SW']
    var_lst = ['SNOW', 'PRCP']
    for country in country_lst:
        for var in var_lst:
            readStreamData(country, var, amount, us_precipitation)


def mlib_data(df, country):
    df = df.filter("Variable == 'TMAX' or Variable == 'TMIN' or Variable == 'SNOW' ").withColumn("month", F.month("fullDate"))
    df = df.filter("month == '1' or month == '2'")
    df = df.withColumn("year", F.year("fullDate"))
    df = df.groupBy(['StationId', 'year', 'month']).pivot('Variable').agg(F.avg("value").alias('avg'),
                                                                          F.count("value").alias('count')).show()
    write_to_server(df,"mlib_data")



def main():
    amount = 5000000
    readStreamData('CA', amount, mlib_data)


if __name__ == '__main__':
    main()
