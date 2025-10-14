package wood.mike;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import wood.mike.util.Config;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Demonstrates using a Custom Partitioner to control message routing and
 * adding Headers for metadata and traceability.
 * This example makes use of the CustomPartitioner.
 */
public class CustomPartitionerProducer {
    public static void main(String[] args) {
        System.out.println("Starting Kafka Producer (Custom Partitioner & Headers Demo)...");

        Properties props = getProperties();

        try (KafkaProducer<String, Product> producer = new KafkaProducer<>(props)) {

            // Create 10 sample products
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, Product> record = getStringProductProducerRecord(i);

                // *** Add Headers for Metadata ***
                record.headers().add(new RecordHeader("source-system", "ERP-V1".getBytes(StandardCharsets.UTF_8)));
                record.headers().add(new RecordHeader("priority", "HIGH".getBytes(StandardCharsets.UTF_8)));

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        System.out.printf("Sent Record: Key=%s, Partition=%d, Offset=%d, Value=%s%n",
                                record.key(), metadata.partition(), metadata.offset(), record.value().name());
                    } else {
                        System.err.println("Error sending message: " + exception.getMessage());
                        exception.printStackTrace();
                    }
                });
            }

            producer.flush();
            System.out.println("Finished sending 10 records.");
        } catch (Exception e) {
            System.err.println("Producer failed: " + e.getMessage());
        }
    }

    private static ProducerRecord<String, Product> getStringProductProducerRecord(int i) {
        String productId = "PROD-XYZ-" + (i % 3); // Use only 3 unique keys to show partitioning

        Product product = new Product(
                productId,
                "Widget " + i,
                100.00 + (i * 10),
                50 + i
        );

        return new ProducerRecord<>(
                Config.PRODUCT_EVENTS_TOPIC,
                productId, // Key ensures ordering for a specific product ID
                product
        );
    }

    private static Properties getProperties() {
        Properties props = Config.commonProperties();
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        // *** use the Custom Partitioner Class ***
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        return props;
    }
}
