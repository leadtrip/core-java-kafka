package wood.mike.simple;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import wood.mike.util.Config;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static wood.mike.util.Config.SIMPLE_TOPIC;

public class SimpleConsumer {

    public static void main(String[] args) {
        Properties properties = getProperties();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

            consumer.subscribe(Collections.singletonList(SIMPLE_TOPIC));
            System.out.println("Consumer subscribed to topic: " + SIMPLE_TOPIC + ". Polling for records...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                records.forEach(record -> {
                    System.out.println("Received: Key=" + record.key()
                            + ", Value=" + record.value()
                            + ", Partition=" + record.partition()
                            + ", Offset=" + record.offset());

                    System.out.print("  Headers: [");
                    for (Header header : record.headers()) {
                        String value = new String(header.value(), StandardCharsets.UTF_8);
                        System.out.printf("%s=%s, ", header.key(), value);
                    }
                    System.out.println("]");
                });

            }
        } catch (Exception e) {
            System.err.println("Consumer encountered an exception: " + e.getMessage());
        }
    }

    private static Properties getProperties() {
        Properties properties = Config.commonProperties();
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return properties;
    }
}
