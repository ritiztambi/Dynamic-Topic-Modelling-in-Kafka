#!/bin/bash

TOPICS=$(/usr/local/Cellar/kafka/2.0.0/libexec/bin/kafka-topics.sh --zookeeper localhost:2181 --list )

for T in $TOPICS
do
  if [ "$T" != "__consumer_offsets" ]; then
    /usr/local/Cellar/kafka/2.0.0/libexec/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic $T
  fi
done
