#!/bin/bash

set -a
KAFKA_TOPIC="netflows"
DATASET_LOCATION="../dataset/nsl-kdd"
MODELS_LOCATION="../Models"
TRAINING_FILE="KDDTrain+.txt"
ML_ALGORITHM="rf" # rf for Random forest or dt for Decision Tree
OUTPUT_METHOD="elasticsearch"
set +a

# Command run program
docker-compose -f docker-compose.yml up
