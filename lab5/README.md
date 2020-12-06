
# Spark RDD Lab Work (Lab #5)

#### Configuration description:
 - upload files from [here](https://www.kaggle.com/usdot/flight-delays) to Google Cloud Storage bucket
 - get this repo (execute on main VM on DataProc):
```
git clone https://github.com/RMDarth/BigData-ProCamp.git
cd BigData-ProCamp/lab5/
chmod +x submit_to_spark.sh upload_to_hdfs.sh
```
- build spark app jar:
```
mvn clean package -f SparkLab5/pom.xml
cp SparkLab5/target/SparkLab5-1.0.jar .
```
- get files to HDFS from Google Cloud Storage bucket:
```
./upload_to_hdfs.sh -g gs://<path_to_folder_with_csv_files>
```
- run spark job and get most popular destination airports by month
```
./submit_to_spark.sh
```
