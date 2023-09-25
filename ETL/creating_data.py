# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession


# COMMAND ----------

# connect to azure data lake using direct configurations
spark.conf.set(
    f"fs.azure.account.key.bentalebstorageacc.dfs.core.windows.net", 
    "lAFhPBYgmGBlkcaW/xObvOI7lrDKAc7UdNgLilVuxHhvBUAlCxo5hBGcuDtvjGeh7M6cT5v5THEu+ASt8S3WoA=="
)
raw = "abfss://publictransportdata@bentalebstorageacc.dfs.core.windows.net/raw/"
processed = "abfss://publictransportdata@bentalebstorageacc.dfs.core.windows.net/raw/"

# COMMAND ----------

dbutils.fs.unmount("/mnt/publictransportdata/")
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

# Generate data for January 2023
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 1, 31)
date_generated = [start_date + timedelta(days=x) for x in range(0, (end_date-start_date).days)]

transport_types = ["Bus", "Train", "Tram", "Metro"]
routes = ["Route_" + str(i) for i in range(1, 11)]
stations = ["Station_" + str(i) for i in range(1, 21)]

# Randomly select 5 days as extreme weather days
extreme_weather_days = random.sample(date_generated, 5)

data = []

for date in date_generated:
    for _ in range(32):  # 32 records per day to get a total of 992 records for January
        transport = random.choice(transport_types)
        route = random.choice(routes)

        # Normal operating hours
        departure_hour = random.randint(5, 22)
        departure_minute = random.randint(0, 59)

        # Introducing Unusual Operating Hours for buses
        if transport == "Bus" and random.random() < 0.05:  # 5% chance
            departure_hour = 3

        departure_time = f"{departure_hour:02}:{departure_minute:02}"

        # Normal duration
        duration = random.randint(10, 120)

        # Introducing Short Turnarounds
        if random.random() < 0.05:  # 5% chance
            duration = random.randint(1, 5)

        # General delay
        delay = random.randint(0, 15)

        # Weather Impact
        if date in extreme_weather_days:
            # Increase delay by 10 to 60 minutes
            delay += random.randint(10, 60)

            # 10% chance to change the route
            if random.random() < 0.10:
                route = random.choice(routes)

        total_minutes = departure_minute + duration + delay
        arrival_hour = departure_hour + total_minutes // 60
        arrival_minute = total_minutes % 60
        arrival_time = f"{arrival_hour:02}:{arrival_minute:02}"

        passengers = random.randint(1, 100)
        departure_station = random.choice(stations)
        arrival_station = random.choice(stations)

        data.append([date, transport, route, departure_time, arrival_time, passengers, departure_station, arrival_station, delay])

month_i = "01"
df = pd.DataFrame(data, columns=["Date", "TransportType", "Route", "DepartureTime", "ArrivalTime", "Passengers", "DepartureStation", "ArrivalStation", "Delay"])

df.to_csv("/mnt/publictransportdata/raw/rawTransportDataOf_"+ month_i + ".csv" , index=False)

# sparkdf = spark.createDataFrame(df)
# output_path = "/raw/" + month_i + ".csv"
# # Write the DataFrame as a CSV file to the mounted Data Lake Storage
# sparkdf.coalesce(1).write.mode("overwrite").option("header", "true").format("com.databricks.spark.csv").save(output_path)
