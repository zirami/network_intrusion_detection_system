#!/bin/bash

set -a
KAFKA_TOPIC="netflows"
DATASET_LOCATION="/home/sparkmaster/Desktop/thangnt7ir/Datasets/NSL-KDD"
MODELS_LOCATION="/home/sparkmaster/Desktop/Models"
TRAINING_FILE="KDDTrain+.txt"
ML_ALGORITHM="rf" # rf cho Random forest hoac dt cho Decision Tree
OUTPUT_METHOD="elasticsearch"
set +a

# Lenh khoi dong docker
docker-compose -f docker-compose.yml up
