
# Hadoop Hive Lab Work (Lab #4)

#### Configuration description:
 - upload files from [here](https://www.kaggle.com/usdot/flight-delays) to Google Cloud Storage bucket
 - get this repo (execute on main VM on DataProc):
```
git clone https://github.com/RMDarth/BigData-ProCamp.git
cd BigData-ProCamp/lab4/
chmod +x upload_to_hdfs.sh
```
- get files to HDFS from Google Cloud Storage bucket:
```
./upload_to_hdfs.sh -g gs://<path_to_folder_with_csv_files>
```
- run hive script and get Top 5 airlines with highest average departure delay:
```
hive -f top5airlines.hql
```

Hive script should create a database, create external tables based on CSV files struct from Google Cloud Storage, and run query to calculate top 5 airlines with biggest average DEPARTURE_DELAY.
