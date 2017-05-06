#!/bin/bash 

USER=$1

if [ -z ${USER} ]; then
    echo "Please provide your HDFS user name"
    exit 1
fi 

spark-submit --master yarn-client --driver-memory 2g --num-executors 3 --executor-memory 8g --conf spark.executor.cores=5 spark-sql-hw.jar /common/training/motels.home/bids.parquet /common/training/motels.home/motels.parquet /common/training/motels.home/exchange_rates /user/$USER/spark-sql-output
