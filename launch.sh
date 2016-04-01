#!/bin/bash
function check_for_kafka_image() {
if [[ "$(docker images -q dnvriend/kafka 2> /dev/null)" == "" ]];
then
    cd kafka
    ./docker-build.sh
    cd ..
fi
}
export IP_VM_DEV=$(docker-machine ip dev)
check_for_kafka_image
docker rm -f $(docker ps -aq)
docker-compose build --no-cache
docker-compose up -d
echo "Launching Kafka..."