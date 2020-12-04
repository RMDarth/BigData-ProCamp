
# Hadoop Map-Reduce Lab Work (Lab #3)

#### Configuration description:
 - upload files from [here](https://www.kaggle.com/usdot/flight-delays) to Google Cloud Storage bucket
 - get this repo (execute on main VM on DataProc):
```
git clone https://github.com/RMDarth/BigData-ProCamp.git
cd BigData-ProCamp/lab3/
chmod +x submit_map_reduce.sh upload_to_hdfs.sh
```
- build map-reduce jar, run unit tests (alternatively there is a prebuilt jar):
```
mvn clean package -f AirlineDelay/pom.xml
cp AirlineDelay/target/AirlineDelay-1.0.jar .
```
- get files to HDFS from Google Cloud Storage bucket:
```
./upload_to_hdfs.sh -g gs://<path_to_folder_with_csv_files>
```
- run map-reduce job and get Top 5 airlines with highest average departure delay:
```
./submit_map_reduce.sh
python TopDelayAirlines.py
```

Because several reducers output several files, python script combines their output and gets top 5 airlines. This could be done as additional map-reduce job (with a single reducer for example), but because airlines number is small, a simple script to process output will perform better than spinning new map-reduce job.

#### Alternative implementation:

There is also alternative map-reduce implementation (in "alternative" folder), which doesn't require additional python script to run in the end, and will output a single file with Top 5 airlines. But it resticts reducers number to 1, so it can be slower on some big datasets. 

To build it, second step should be changed:
```
mvn clean package -f alternative/AirlineDelay/pom.xml
cp alternative/AirlineDelay/target/AirlineDelay-1.0.jar .
```
And to run and show result, last step should be changed:
```
./submit_map_reduce.sh
hadoop fs -cat /bdpc/hadoop_mr/airline/output/part*
```
