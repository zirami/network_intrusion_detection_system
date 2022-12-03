#!/bin/bash
#
# Labels needed to be set on swarm nodes for the services:
#   - role=master --> Zookeeper, spark master and spark-submits
#   - role=worker --> Spark workers and kafka brokers
#   - role=esnode --> Elasticsearch nodes,kibana and elasticsearch-kibana setup service
#
#   e.g "docker node update --label-add role=master <Node-Id>
#

set -a
KAFKA_TOPIC="netflows"
NUM_OF_SPARK_WORKERS="2"
MODELS_LOCATION="s3://thangntsbucket/Models/"
TRAINING_FILE_LOCATION="s3://thangntsbucket/Datasets/NSL-KDD/KDDTrain+.txt"
SPARK_TRAIN_ARGUMENTS="--conf spark.hadoop.fs.s3a.endpoint=s3.us-east-1.amazonaws.com --conf spark.hadoop.fs.s3a.access.key=abc --conf spark.hadoop.fs.s3a.secret.key=abc"
ML_ALGORITHM="dt" # rf for Random forest or dt for Decision Tree
OUTPUT_METHOD="elasticsearch" #console or elasticsearch
SPARK_TEST_ARGUMENTS="--conf spark.network.timeout=300 --conf spark.sql.streaming.metricsEnabled=true --conf spark.hadoop.fs.s3a.endpoint=s3.us-east-1.amazonaws.com
 --conf spark.hadoop.fs.s3a.access.key=Aqbq --conf spark.hadoop.fs.s3a.secret.key=acvb"
ELASTICSEARCH_NODE_NAMES="node6.swarm1.network-intrusion-detect-pg0.emulab.net,node7.swarm1.network-intrusion-detect-pg0.emulab.net,nodees.swarm1.network-intrusion-detect-pg0.emulab.net"
set +a

docker stack deploy -c docker-compose-swarm.yml testcluster
