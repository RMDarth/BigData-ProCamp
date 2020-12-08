
# Spark RDD Lab Work (Lab #5)

#### Configuration description:
 - upload files from [here](https://www.kaggle.com/usdot/flight-delays) to Google Cloud Storage bucket
 - install Maven (sudo apt install maven) and [sbt](https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html) to compile Java and Scala code 
 - get this repo (execute on main VM on DataProc):
```
git clone https://github.com/RMDarth/BigData-ProCamp.git
cd BigData-ProCamp/lab5/
chmod +x submit_to_spark_t1.sh submit_to_spark_t2.sh upload_to_hdfs.sh
```
- get files to HDFS from Google Cloud Storage bucket:
```
./upload_to_hdfs.sh -g gs://<path_to_folder_with_csv_files>
```

#### Task 1:
- build spark app jar (Java based):
```
mvn clean package -f SparkLab5Task1/pom.xml
cp SparkLab5Task1/target/SparkLab5-1.0.jar .
```
- run spark job and get most popular destination airports by month
```
./submit_to_spark_t1.sh
```

#### Task 2:
- build spark app jar (Scala based):
```
(cd SparkLab5Task2/; sbt package)
cp SparkLab5Task2/target/scala-2.12/sparklabtask2_2.12-1.0.jar .
```
- run spark job and get json document with canceled flights statistics by airline by airport
```
./submit_to_spark_t2.sh
```
Note: Waco Regional Airport stats will be saved as separate CSV. Accumulators are added to count total flights by each airline.
