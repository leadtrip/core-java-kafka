### Test ground for Kafka with core java libraries

Start kafka:\
`docker compose up -d`

Create topics (probably not needed as auto.create.topics.enable defaults to true with docker image):\
`docker exec broker /opt/kafka/bin/kafka-topics.sh --create --topic simple-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1`
`docker exec broker /opt/kafka/bin/kafka-topics.sh --create --topic product-events-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1`

There's a producer and consumer for each example in the relevant producer and consumer modules.\
Each has a main method, just start the consumer first then the producer, there'll be some sort of log output.