# Databricks notebook source
from datetime import datetime

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

raw_files = dbutils.fs.ls(raw)
raw_csv_files = [f.path for f in raw_files if f.name.endswith(".csv")]

dates = []
for i in range(len(raw_csv_files )):
    # datt = datetime(int(raw_csv_files[i].split("/")[-1].split(".")[0].split("_")[1]),int(raw_csv_files[i].split("/")[-1].split(".")[0].split("_")[2]), 1)
    new_year = raw_csv_files[i].split("/")[-1].split(".")[0].split("_")[1]
    if new_year not in dates:
        dates.append(new_year)

dates
    

