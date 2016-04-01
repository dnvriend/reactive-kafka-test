#!/bin/bash
export ZOOKEEPER=$(docker-machine ip dev):2181
export SCALA_VERSION=2.11
export KAFKA_VERSION=0.9.0.1
export KAFKA_HOME=/opt/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION"
docker exec -it kafka $KAFKA_HOME/bin/kafka-console-consumer.sh --zookeeper $ZOOKEEPER --topic test --from-beginning