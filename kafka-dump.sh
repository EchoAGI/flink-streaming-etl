#!/bin/sh

docker-compose exec kafka /kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --property print.key=true \
    --topic $1
