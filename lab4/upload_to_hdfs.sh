usage() {
  echo -e "Usage: $0 [-g <gs path>] [-d <hdfs path>]\n"\
       "where\n"\
       "-g defines path to a bucket with input csv files\n"\
       "-d defines an hdfs destination path\n"\
       "\n"\
        1>&2
  exit 1
}

while getopts ":g:d:" opt; do
    case "$opt" in
        g)  GS_PATH=${OPTARG} ;;
        d)  HDFS_PATH=${OPTARG} ;;
        *)  usage ;;
    esac
done

if [[ -z "$HDFS_PATH" ]];
then
  HDFS_PATH="/bdpc/hadoop/lab4"
fi

hadoop fs -rm -R "$HDFS_PATH"
hdfs dfs -mkdir -p "$HDFS_PATH"
hdfs dfs -mkdir -p "${HDFS_PATH}/flights"
hdfs dfs -mkdir -p "${HDFS_PATH}/airlines"

if [[ -z "$GS_PATH" ]];
then
  GS_PATH="gs://barinov_bdpc"
fi


THIS_FILE=$(readlink -f "$0")
THIS_PATH=$(dirname "$THIS_FILE")

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo "THIS_FILE = $THIS_FILE"
echo "THIS_PATH = $THIS_PATH"
echo "BASE_PATH = $BASE_PATH"
echo "-------------------------------------"
echo "HDFS_PATH = $HDFS_PATH"
echo "GS_PATH = $GS_PATH"
echo "-------------------------------------"


UPLOAD_CMD1="hdfs dfs -cp ${GS_PATH}/flights.csv ${HDFS_PATH}/flights/flights.csv"
echo "$UPLOAD_CMD1"
${UPLOAD_CMD1}

UPLOAD_CMD2="hdfs dfs -cp ${GS_PATH}/airlines.csv ${HDFS_PATH}/airlines/airlines.csv"
echo "$UPLOAD_CMD2"
${UPLOAD_CMD2}

echo "<<<<<<<<<<<<<<<<<<  HDFS  <<<<<<<<<<<<<<<<<<<<<"

hdfs dfs -ls -R ${HDFS_PATH}

echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"