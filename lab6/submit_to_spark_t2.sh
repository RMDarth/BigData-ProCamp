
usage() {
  echo -e "Usage: $0 [-f <path>] [-p <path>] [-a <path>] [-o <path>]\n"\
       "where\n"\
       "-f defines an input flights.csv path\n"\
       "-p defines an input airports.csv path\n"\
       "-a defines an input airlines.csv path\n"\
       "-o defines an output folder path\n"\
       "\n"\
        1>&2
  exit 1
}


while getopts ":f:p:a:o" opt; do
    case "$opt" in
        f)  INPUT_PATH=${OPTARG} ;;
        p)  AIRPORTS_PATH=${OPTARG} ;;
        a)  AIRLINES_PATH=${OPTARG} ;;
        o)  OUTPUT_PATH=${OPTARG} ;;
        *)  usage ;;
    esac
done

if [[ -z "$INPUT_PATH" ]];
then
  INPUT_PATH="/bdpc/hadoop/lab6/input/flights.csv"
fi

if [[ -z "$AIRLINES_PATH" ]];
then
  AIRLINES_PATH="/bdpc/hadoop/lab6/airlines/airlines.csv"
fi

if [[ -z "$AIRPORTS_PATH" ]];
then
  AIRPORTS_PATH="/bdpc/hadoop/lab6/airports/airports.csv"
fi

if [[ -z "$OUTPUT_PATH" ]];
then
  OUTPUT_PATH="/bdpc/hadoop/lab6/canceledFlights"
fi

hadoop fs -rm -R $OUTPUT_PATH

echo Submitting job to spark...

spark-submit --master yarn \
             --num-executors 20 --executor-memory 1G --executor-cores 1 --driver-memory 1G \
             --conf spark.ui.showConsoleProgress=true \
             --class com.github.rmdarth.bdpclab6.CanceledFlightsDF \
             sparkdf2_2.12-1.0.jar "$INPUT_PATH" "$AIRPORTS_PATH" "$AIRLINES_PATH" "$OUTPUT_PATH"
hadoop dfs -ls $OUTPUT_PATH
echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
hadoop dfs -cat $OUTPUT_PATH/output_json/*
echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
hadoop dfs -cat $OUTPUT_PATH/output_csv/*
echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"