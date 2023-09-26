# Databricks notebook source
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType,IntegerType
from pyspark.sql.functions import to_date, year, month, day, hour, minute, when, avg, regexp_replace, mean, count, round
from pyspark.sql import SparkSession

# COMMAND ----------

# Mounting data lake
storageAccountName = "bentalebstorageacc"
storageAccountAccessKey = "6eZ31oe7aTRK1+aSifn5vg7AmN/XZ+PWbgMBOqb3O3mt22OrW0jWNld9ZODh5rcs/P5ZEzuEtBs2+AStGnMmQA=="
sasToken = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-09-26T17:26:29Z&st=2023-09-26T09:26:29Z&spr=https&sig=b0aopyN4k73YZ%2B4AeCIeZggqFSrq76bS477XUkkxDAY%3D"
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

raw = f"{mountPoint}raw/"
processed = f"{mountPoint}processed/"

processed_count = 0
raw_files = dbutils.fs.ls(raw)
raw_csv_files = [f.path for f in raw_files if f.name.endswith(".csv")]
raw_file_count = len(raw_csv_files)
processed_files = dbutils.fs.ls(processed + "transport/")
processed_csv_files = [f.path for f in processed_files if f.name.endswith(".csv")]
for i in range(raw_file_count):
    specific_name = "dbfs:" + processed + "transport/"+  raw_csv_files[i].replace("raw", "").split("/")[-1].split(".")[0] + "_processed.csv"
    if processed_count == 2:
        break
    elif specific_name in processed_csv_files:
        continue
    else:
        df = spark.read.format("csv").option("inferSchema", "True").option("header",
        "True").option("delimeter",",").load(raw_csv_files[i])

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
        df = df.withColumn("Duration", when(df["Duration"] < 0, df["Duration"] + 12*60).otherwise(df["Duration"]))

        pandasDF = df.toPandas()
        file_name = f"/dbfs/mnt/publictransportdata/processed/transport/" + raw_csv_files[i].replace("raw", "").split("/")[-1].split(".")[0] + "_processed.csv"
        pandasDF.to_csv(file_name, index=False)

        # Peak hours
        df_peak_hours = df.withColumn("DepartureHour", hour(df["ArrivalTime"]))
        df_peak_hours = df_peak_hours.groupBy("DepartureHour").agg(
            round(mean("Passengers"), 2).alias("AvgPassengers"))
        # Identify peak and off-peak times based on passenger numbers
        avg_passengers = df.select(avg(df["Passengers"])).collect()[0][0]
        df_peak_hours = df_peak_hours.withColumn("Peak_hour", df_peak_hours["AvgPassengers"] >= avg_passengers)

        pandasDF = df_peak_hours.toPandas()
        file_name = f"/dbfs/mnt/publictransportdata/processed/peakhour/peakhour" + raw_csv_files[i].replace("raw", "").split("/")[-1].split(".")[0] + "_processed.csv"
        pandasDF.to_csv(file_name, index=False)

        # routes analysis
        df_avg = df.groupBy("Route").agg(
            round(mean("Passengers"), 2).alias("AvgPassengers"),
            round(mean("Delay"), 2).alias("AvgDelay"),
            round(count("Route"), 2).alias("Trips")
        )

        pandasDF = df_avg.toPandas()
        file_name = f"/dbfs/mnt/publictransportdata/processed/routes/routes" + raw_csv_files[i].replace("raw", "").split("/")[-1].split(".")[0] + "_processed.csv"
        pandasDF.to_csv(file_name, index=False)
        
        processed_count += 1
        print("processed Month - " + str(i + 1))


# COMMAND ----------

dbutils.fs.unmount("/mnt/publictransportdata/")
