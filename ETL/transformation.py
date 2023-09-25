# Databricks notebook source
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType 
from pyspark.sql.functions import to_date, year, month, day, hour, minute, when, avg, regexp_replace, mean, count, round
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession

# COMMAND ----------

# Mounting data lake
storageAccountName = "bentalebstorageacc"
storageAccountAccessKey = "lAFhPBYgmGBlkcaW/xObvOI7lrDKAc7UdNgLilVuxHhvBUAlCxo5hBGcuDtvjGeh7M6cT5v5THEu+ASt8S3WoA=="
sasToken = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-09-25T23:39:50Z&st=2023-09-25T15:39:50Z&spr=https&sig=uF4HRUl2j6IFpEe%2BIziEjACYZdWaK9hq8UZ0C1HjXqk%3D"
blobContainerName = "publictransportdata"
mountPoint = "/mnt/publictransportdata/"
if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
      source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
      mount_point = mountPoint,
      extra_configs = {'fs.azure.sas.' + blobContainerName + '.' + storageAccountName + '.blob.core.windows.net': sasToken}
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

raw = "/dbfs/mnt/publictransportdata/raw/"
processed = "/dbfs/mnt/publictransportdata/processed/"


# COMMAND ----------


pddf =  pd.read_csv(raw + "rawTransportDataOf_01_2023.csv")
#Create PySpark SparkSession
spark = SparkSession.builder.appName("PandasToSparkDF").getOrCreate()
#Create PySpark DataFrame from Pandas
df = spark.CreateDataFrame(pddf)

# year month day
df = df.withColumn("Year", year(df.Date))
df = df.withColumn("Month",month(df.Date))
df = df.withColumn("Day", day(df.Date))

# Change Date column to datetime
df = df.withColumn("Date", to_date(df["Date"]))

# Add DelayCategory column
df = df.withColumn("DelayCategory",
    when(df["Delay"] <= 0, "On Time")
    .when(df["Delay"] <= 10, "Short Delay")
    .when(df["Delay"] <= 20, "Medium Delay")
    .otherwise("Long Delay")
)

# Add Duration column
df = df.withColumn("ArrivalTime", regexp_replace(df["ArrivalTime"], "24:", "00:"))
df = df.withColumn("ArrivalTime", regexp_replace(df["ArrivalTime"], "25:", "00:"))
df = df.withColumn("Duration", (hour(df["ArrivalTime"]) * 60 + minute(df["ArrivalTime"])) - (hour(df["DepartureTime"]) * 60 + minute(df["DepartureTime"])))
df = df.withColumn("Duration", when(df["Duration"] < 0, df["Duration"] + 24*60).otherwise(df["Duration"]))

# Show the resulting DataFrame
df.show()

# COMMAND ----------

df_peak_hours = df.withColumn("DepartureHour", hour(df["ArrivalTime"]))
df_peak_hours = df_peak_hours.groupBy("DepartureHour").agg(
    round(mean("Passengers"), 2).alias("AvgPassengers")
)
# Identify peak and off-peak times based on passenger numbers
avg_passengers = df.select(avg(df["Passengers"])).collect()[0][0]
df_peak_hours = df_peak_hours.withColumn("Peak_hour", df_peak_hours["AvgPassengers"] >= avg_passengers)

df_peak_hours.show()


# COMMAND ----------

df_avg = df.groupBy("Route").agg(
    round(mean("Passengers"), 2).alias("AvgPassengers"),
    round(mean("Delay"), 2).alias("AvgDelay"),
    round(count("Route"), 2).alias("Trips")
)

df_avg.show()

# COMMAND ----------

dbutils.fs.unmount("/mnt/publictransportdata/")
