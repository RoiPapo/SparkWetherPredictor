
from pyspark.sql import SparkSession
import os

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


def main():
    spark = SparkSession.builder.appName('dfdfd').getOrCreate()

    station_df = spark.read.text('ghcnd-stations.txt')
    station_df = station_df.select(station_df.value.substr(0, 11).alias('StationId'),
                                   station_df.value.substr(12, 9).cast("float").alias('latitude'),
                                   station_df.value.substr(22, 9).cast("float").alias('longitude'),
                                   station_df.value.substr(32, 7).cast("float").alias('longitude'))
    write_to_server(station_df, 'stations_test')


if __name__ == '__main__':
    main()