### Test ground for Kafka including streams with core java libraries

Start kafka:\
`docker compose up -d`

There's a producer and consumer for each example in the relevant producer and consumer modules.\
`SimpleProducer` -> `SimpleConsumer`\
`ProductProducer` -> `ProductConsumer` -> `ProductStreamProcessor`\
`CustomPartitionerProducer` -> `MetadataConsumer`

Each has a main method, just start the consumer first then the producer, there'll be some sort of log output.

### Useful queries
List all topics:\
`docker exec broker /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092`

Describe a single topic:\
`docker exec broker /opt/kafka/bin/kafka-topics.sh --describe --topic product-events --bootstrap-server localhost:9092`
