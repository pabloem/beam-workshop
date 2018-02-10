#!/bin/bash

cat << EOF
Set up the local environment for a full workshop on Apache Beam.
usage: setup.sh [-all|-h]
    flags:
        -all - Set up ALL the environment variables, instead of just the basic ones.
        -h   - Show this help.
EOF

if [ "$1" = "-h" ]
then
  return 1;
fi

CHANGE_ALL_VALUES=false
if [ "$1" = "-all" ]
then
  CHANGE_ALL_VALUES=true
fi

####################################
# DEFAULT VALUES FOR ENV VARIABLES
####################################
_DEFAULT_GCP_PROJECT=beam-workshop-mex
_DEFAULT_LOCAL_FILE=$PWD/data/demo-file.csv

_DEFAULT_PUBSUB_TOPIC=bwbeta
_DEFAULT_KAFKA_IP=127.0.0.1:1234
_DEFAULT_FLINK_MASTER=35.194.11.109:40007
_DEFAULT_SPARK_IP=35.188.104.76
_DEFAULT_GCP_INPUT_FILE=gs://apache-beam-demo/data/gaming*
_DEFAULT_GCP_OUTPUT_FILE=gs://beam-workshop-outputs/$USER/gaming

function readOrDefault {
  REQUESTED_VALUE_NAME=$1
  DEFAULT_VALUE=$2
  ACCEPT_USER_INPUT=$3
  if [ "$ACCEPT_USER_INPUT" = "true" ]
  then
    read -p "Enter ${REQUESTED_VALUE_NAME} [${DEFAULT_VALUE}]: " USER_VALUE
    >&2 echo "Set to ${USER_VALUE:=$DEFAULT_VALUE}"
    >&2 echo ""
  fi
  echo ${USER_VALUE:=$DEFAULT_VALUE}
}

#####################################
# SETTING MAIN ENVIRONMENT VARIABLES
#####################################
GCP_PROJECT=`readOrDefault "GCP Project ID" $_DEFAULT_GCP_PROJECT true`
GCP_OUTPUT_FILE=`readOrDefault "Output Prefix" $_DEFAULT_GCP_OUTPUT_FILE true`

#####################################
# SETTING SECONDARY ENVIRONMENT VARIABLES
#####################################
GCP_INPUT_FILE=`readOrDefault "Input File in GCP" $_DEFAULT_GCP_INPUT_FILE $CHANGE_ALL_VALUES`
BEAM_LOCAL_FILE=`readOrDefault "Local Input File" $_DEFAULT_LOCAL_FILE $CHANGE_ALL_VALUES`
PUBSUB_TOPIC=`readOrDefault "PubSub Topic" $_DEFAULT_PUBSUB_TOPIC $CHANGE_ALL_VALUES`
KAFKA_IP_ADDRESS=`readOrDefault "Kafka Cluster Address (IP+port)" $_DEFAULT_KAFKA_IP $CHANGE_ALL_VALUES`
FLINK_MASTER_ADDRESS=`readOrDefault "Flink Cluster Address (IP+port)" $_DEFAULT_FLINK_MASTER $CHANGE_ALL_VALUES`
SPARK_MASTER_IP=`readOrDefault "Spark Cluster IP Address (IP only)" $_DEFAULT_SPARK_IP $CHANGE_ALL_VALUES`


#####################################
# SETTING UP PYTHON ENVIRONMENT
#####################################
read -r -p "Setup Python Environment? [y/N] " response
case "$response"
  in [yY][eE][sS]|[yY])
    pip install virtualenv && virtualenv -p python2.7 .venv
    source .venv/bin/activate
    pip install apache-beam[gcp]
    ;;
esac


#####################################
# SETTING UP GCLOUD ENVIRONMENT
#####################################
read -r -p "Google Cloud Command Line Interface? [y/N] " response
case "$response"
  in [yY][eE][sS]|[yY])
    curl http://sdk.cloud.google.com | bash
    ;;
esac
