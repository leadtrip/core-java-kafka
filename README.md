### Test ground for Kafka with core java libraries

Start kafka:\
`docker compose up -d`

Create topics:\
`docker exec broker /opt/kafka/bin/kafka-topics.sh --create --topic simple-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1`
`docker exec broker /opt/kafka/bin/kafka-topics.sh --create --topic product-events-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1`

Verify topic has been created:\
`docker exec broker /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092`

Start the consumer:\
`./gradlew :consumer-app:run`

Start the producer:\
`./gradlew :producer-app:run`