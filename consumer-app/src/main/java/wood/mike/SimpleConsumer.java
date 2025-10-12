package wood.mike;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleConsumer {

    public static void main(String[] args) {
        Properties properties = getProperties();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

            String topic = "random-topic";

            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Consumer subscribed to topic: " + topic + ". Polling for records...");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                records.forEach(record -> {
                    System.out.println("Received: Key=" + record.key()
                            + ", Value=" + record.value()
                            + ", Partition=" + record.partition()
                            + ", Offset=" + record.offset());
                });

                // Optional: manually commit offsets (auto-commit is default, but manual is better practice)
                // consumer.commitSync();
            }
        } catch (Exception e) {
            System.err.println("Consumer encountered an exception: " + e.getMessage());
        }
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-java-consumer-group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }
}
