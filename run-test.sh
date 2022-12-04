#!/bin/bash
set -a
KAFKA_TOPIC="netflows"
NUM_OF_SPARK_WORKERS="2"
MODELS_LOCATION="s3a://thangntsbucket/Models"
DATASET_LOCATION="s3a://thangntsbucket/Datasets/NSL-KDD"
TRAINING_FILE="KDDTrain+.txt"
SPARK_TRAIN_ARGUMENTS="--conf spark.hadoop.fs.s3a.endpoint=s3.us-east-1.amazonaws.com --conf spark.hadoop.fs.s3a.access.key=<access_key> --conf spark.hadoop.fs.s3a.secret.key=<secret_key>"
ML_ALGORITHM="dt" # rf for Random forest or dt for Decision Tree
OUTPUT_METHOD="elasticsearch" #console or elasticsearch
SPARK_TEST_ARGUMENTS="--conf spark.network.timeout=300 --conf spark.sql.streaming.metricsEnabled=true --conf spark.hadoop.fs.s3a.endpoint=s3.us-east-1.amazonaws.com
 --conf spark.hadoop.fs.s3a.access.key=<access_key> --conf spark.hadoop.fs.s3a.secret.key=<secret_key>"
ELASTICSEARCH_NODE_NAMES="node6.swarm1.network-intrusion-detect-pg0.emulab.net,node7.swarm1.network-intrusion-detect-pg0.emulab.net,nodees.swarm1.network-intrusion-detect-pg0.emulab.net"
set +a

# Lenh khoi dong docker
docker-compose -f docker-compose-testconnect.yml up
