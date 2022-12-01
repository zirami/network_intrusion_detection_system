#!/bin/bash

set -a
KAFKA_TOPIC="netflows"
DATASET_LOCATION="../dataset/nsl-kdd"
MODELS_LOCATION="../Models"
TRAINING_FILE="KDDTrain+.txt"
ML_ALGORITHM="rf" # rf cho Random forest hoac dt cho Decision Tree
OUTPUT_METHOD="elasticsearch"
set +a

# Lenh khoi dong docker
docker-compose -f docker-compose.yml up
