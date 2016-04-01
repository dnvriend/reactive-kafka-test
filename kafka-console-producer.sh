#!/bin/bash
export KAFKA=$(docker-machine ip dev):9092
export SCALA_VERSION=2.11
export KAFKA_VERSION=0.9.0.1
export KAFKA_HOME=/opt/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION"
docker exec -it kafka $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list $KAFKA --topic test