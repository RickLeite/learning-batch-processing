
set -e  # "exit on error" 

# $# -> nº arguments passed
if [ $# -lt 2 ]; then
    echo "Usage: $0 taxi_type year"
    exit 1
fi

TAXI_TYPE="$1"
YEAR="$2"

URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"


for MONTH in {1..12}; do
  FMONTH=`printf "%02d" ${MONTH}`

  URL="${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"
  #https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_20""-"".csv.gz

  # Testing if URL exists
  if wget --spider ${URL} ; then
    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

    echo "downloading ${URL} to ${LOCAL_PATH}"
    mkdir -p ${LOCAL_PREFIX}
    wget ${URL} -O ${LOCAL_PATH}
    echo "Sucess downloading"
    current_datetime=$(date +"%Y-%m-%d %H:%M:%S")
    echo "${current_datetime} Sucess downloading ${TAXI_TYPE}-${YEAR}-${FMONTH}" >> "data/${TAXI_TYPE}-${YEAR}-STATUS.txt"
  else
    echo "URL does not exist"
  fi

done