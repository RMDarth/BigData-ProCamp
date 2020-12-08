
usage() {
  echo -e "Usage: $0 [-i <path>] [-o <path>]\n"\
       "where\n"\
       "-i defines an input flights.csv path\n"\
       "-o defines an output folder path\n"\
       "\n"\
        1>&2
  exit 1
}


while getopts ":i:o" opt; do
    case "$opt" in
        i)  INPUT_PATH=${OPTARG} ;;
        o)  OUTPUT_PATH=${OPTARG} ;;
        *)  usage ;;
    esac
done

if [[ -z "$INPUT_PATH" ]];
then
  INPUT_PATH="/bdpc/hadoop/lab5/input/flights.csv"
fi

if [[ -z "$OUTPUT_PATH" ]];
then
  OUTPUT_PATH="/bdpc/hadoop/lab5/popularAirports"
fi

hadoop fs -rm -R $OUTPUT_PATH

echo Submitting job to spark...

spark-submit --master yarn \
             --num-executors 20 --executor-memory 1G --executor-cores 1 --driver-memory 1G \
             --conf spark.ui.showConsoleProgress=true \
             --class com.github.rmdarth.bdpclab5.PopularAirportProcessor \
             SparkLab5-1.0.jar "$INPUT_PATH" "$OUTPUT_PATH"
hadoop dfs -cat $OUTPUT_PATH/*

echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"