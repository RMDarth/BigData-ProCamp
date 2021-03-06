
# Apache Airflow orchestration Lab Work (Lab #8)

#### Configuration description:
 - this workflow uses 2 separate jars (as 2 tasks from lab 6 were created as separate projects), so it will run 2 tasks in parallel
 - either build jars from lab 6 or use prebuilt fat jars from [here](https://drive.google.com/drive/folders/1CIXA7zLuWkq2D8mNTyT6IAhDNeJeW9Je?usp=sharing) (Scala 2.11 version)
 - initialize Composer cluster
 - upload files to Google Cloud Storage: 
    * create bucket in GCS
    * upload all 3 flights related CSV files into the bucket, into /flights/input folder
    * upload jars (from "jars" folder) into the bucket, into /jars folder
 - go to Airflow UI and setup Variables:
    * "gce_zone" -> set zone code where work cluster will be created (e.g. "us-central1-c")
    * "gcp_project" -> project id for Google DataProc cluster
    * "gcs_bucket" -> bucket to use for input and output data
 - upload DAG with lab workflow - file **`lab8_orchestration.py`** into Composer cluster
 - check Airflow UI, it should now contain new flow with hourly execution

DAG has sensor which check that file with format `gc://${bucket_name}/flights/${yyyy}/${MM}/${dd}/${HH}/_SUCCESS` exists, so in order to completely execute the workflow, such file should be created with current datetime folders.

Output for 2 Spark jobs will be stored in provided GCS bucket, in folders flights/pop_airports/${datetime} and flights/canceled/${datetime}