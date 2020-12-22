
usage() {
  echo -e "Usage: $0 [-g <bucket>] [-f <folder>] [-t <topic>]\n"\
       "where\n"\
       "-g defines a GCP Storage Bucket for ouput\n"\
       "-f defines an output folder inside bucket\n"\
       "-t defines a kafka topic to read\n"\
       "\n"\
        1>&2
  exit 1
}


while getopts ":g:f:t" opt; do
    case "$opt" in
        g)  GCP_BUCKET=${OPTARG} ;;
        f)  OUTPUT_FOLDER=${OPTARG} ;;
        t)  KAFKA_TOPIC=${OPTARG} ;;
        *)  usage ;;
    esac
done

if [[ -z "$GCP_BUCKET" ]];
then
  GCP_BUCKET="barinov_btc"
fi

if [[ -z "$OUTPUT_FOLDER" ]];
then
  OUTPUT_FOLDER="btc"
fi

if [[ -z "$KAFKA_TOPIC" ]];
then
  KAFKA_TOPIC="btc_stock"
fi

echo Submitting job to spark...

spark-submit --master yarn \
             --num-executors 20 --executor-memory 1G --executor-cores 1 --driver-memory 1G \
             --conf spark.ui.showConsoleProgress=true \
             --class com.github.rmdarth.bdpclab7.BtcAggregation \
             --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.7 \
	     sparkstreaming_2.12-1.0.jar "$GCP_BUCKET" "$OUTPUT_FOLDER" "$KAFKA_TOPIC"

echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
