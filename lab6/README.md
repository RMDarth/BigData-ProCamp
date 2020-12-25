
# Spark DataFrames Lab Work (Lab #6)

#### Configuration description:
 - upload files from [here](https://www.kaggle.com/usdot/flight-delays) to Google Cloud Storage bucket
 - install [sbt](https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html) to compile Scala code (or use prebuilt jars in "prebuilt" folder or fat jars from [here](https://drive.google.com/drive/folders/16V2mz-AcnIqeBBPVCqkDrroKLZMoanQ1?usp=sharing) )
 - get this repo (execute on main VM on DataProc):
```
git clone https://github.com/RMDarth/BigData-ProCamp.git
cd BigData-ProCamp/lab6/
chmod +x submit_to_spark_t1.sh submit_to_spark_t2.sh upload_to_hdfs.sh
```
- get files to HDFS from Google Cloud Storage bucket:
```
./upload_to_hdfs.sh -g gs://<path_to_folder_with_csv_files>
```

Uploading files is optional. Scripts below can use files directly from GCS, using arguments `-f gs://<path_to_flights.csv> -p gs://<path_to_airports.csv>` and for second task additionaly `-a gs://<path_to_airlines.csv>`

#### Task 1:
- build spark app jar (or use prebuilt):
```
(cd SparkDF1/; sbt test package)
cp SparkDF1/target/scala-2.12/sparkdf1_2.12-1.0.jar .
```
- run spark job and get most popular destination airports by month
```
./submit_to_spark_t1.sh
```

#### Task 2:
- build spark app jar (or use prebuilt):
```
(cd SparkDF2/; sbt test package)
cp SparkDF2/target/scala-2.12/sparkdf2_2.12-1.0.jar .
```
- run spark job and get json document with canceled flights statistics by airline by airport
```
./submit_to_spark_t2.sh
```
Note: Waco Regional Airport stats will be saved as separate CSV. Accumulators are added to count total flights by each airline.
