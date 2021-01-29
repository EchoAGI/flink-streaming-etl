#!/bin/sh

docker-compose exec kafka /kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 --list