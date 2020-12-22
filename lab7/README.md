
# Spark Streaming Lab Work (Lab #7)

#### Configuration description:
 - install [sbt](https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Linux.html) to compile Scala code (or use prebuilt jars in "prebuilt" folder)
 - get this repo (execute on main VM on DataProc):
```
git clone https://github.com/RMDarth/BigData-ProCamp.git
cd BigData-ProCamp/lab7/
chmod +x submit_to_spark.sh create_topic.sh
```
 - build Spark job (or use prebuilt):
 ```
(cd SparkStreaming/; sbt package)
cp SparkStreaming/target/scala-2.12/sparkstreaming_2.12-1.0.jar .
```
 - create Kafka topic
 ```
 ./create_topic.sh
 ```
 - configure NiFi (see ../lab2 for details). Upload template **`NiFiPublishBtcStockToKafka.xml`**, configure and enable Controller Services (set Truststore password to "truststore" in StandardRestrictedSSLContextService), set global variable "CurrencyPair" to "btcusd" value, run all processors.
 - run Spark streaming job:
  ```
 ./submit_to_spark.sh -g <gcp_bucket_name>
 ```

Output is stored as json files in GCP Storage bucket (in folder btc, or user provided). Each json is 1 minute window aggregated data (with 3 min latency).
Checkpoint for output sink is stored here in hdfs: /bdpc/lab7/checkpoint