package wood.mike;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import wood.mike.product.Product;
import wood.mike.util.Config;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MetadataConsumer {
    private static final String GROUP_ID = "metadata-consumer-group";

    public static void main(String[] args) {
        System.out.println("Starting Kafka Metadata Consumer (Headers Demo)...");

        Properties props = getProperties();

        try (KafkaConsumer<String, Product> consumer = new KafkaConsumer<>(
                props,
                new StringDeserializer(),
                new JsonDeserializer<>(Product.class))) {

            consumer.subscribe(Collections.singletonList(Config.PRODUCT_EVENTS_TOPIC));
            System.out.println("Subscribed to topic " + Config.PRODUCT_EVENTS_TOPIC + ". Waiting for metadata events...");

            while (true) {
                ConsumerRecords<String, Product> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, Product> record : records) {
                    System.out.printf("--- Record Received ---%n");
                    System.out.printf("  Partition: %d, Offset: %d, Key: %s%n",
                            record.partition(), record.offset(), record.key());
                    System.out.printf("  Product: ID=%s, Name='%s', Price=%.2f%n",
                            record.value().id(), record.value().name(), record.value().price());

                    System.out.print("  Headers: [");
                    for (Header header : record.headers()) {
                        String value = new String(header.value(), StandardCharsets.UTF_8);
                        System.out.printf("%s=%s, ", header.key(), value);
                    }
                    System.out.println("]");
                }
            }
        } catch (Exception e) {
            System.err.println("Consumer interrupted or encountered a fatal error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static Properties getProperties() {
        Properties props = Config.commonProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        return props;
    }
}
