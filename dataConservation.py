# Databricks notebook source
from datetime import datetime

# COMMAND ----------

# Mounting data lake
storageAccountName = "bentalebstorageacc"
storageAccountAccessKey = "GywYlyJdlkIqmpXSCXYPrrb0GpGAmTOxtBGDs7XuHMN4560s7BFpx6m50iG0Q1ZVj0oPiV2mtqVC+AStqFxSTw=="
sasToken = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-09-27T15:43:46Z&st=2023-09-27T07:43:46Z&spr=https&sig=j1eZSlIJwwSBiiarU%2BqjILJ0nSIopL%2BS86m82Mlry24%3D"
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

#Disable server side copy
spark.conf.set("databricks.spark.dbutils.fs.cp.server-side.enabled", "false")

raw = f"{mountPoint}raw/"
archive = f"{mountPoint}archive/"

raw_files = dbutils.fs.ls(raw)
raw_csv_files = [f.path for f in raw_files if f.name.endswith(".csv")]
raw_csv_files.sort()

# delete the oldet file
dbutils.fs.cp(raw_csv_files[0], archive)
dbutils.fs.rm(raw_csv_files[0])
dbutils.fs.unmount(mountPoint)
