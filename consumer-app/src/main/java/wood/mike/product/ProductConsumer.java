package wood.mike.product;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import wood.mike.JsonDeserializer;
import wood.mike.util.Config;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static wood.mike.util.Config.PRODUCT_EVENTS_TOPIC;


public class ProductConsumer {

    public static void main(String[] args) {
        System.out.println("Starting Kafka Product Consumer...");
        new ProductConsumer().run();
    }

    private void run() {
        Properties props = getProperties();

        try (KafkaConsumer<String, Product> consumer = new KafkaConsumer<>(
                props,
                new StringDeserializer(),
                new JsonDeserializer<>(Product.class))) {

            consumer.subscribe(Collections.singletonList(PRODUCT_EVENTS_TOPIC));

            System.out.println("Subscribed to topic " + PRODUCT_EVENTS_TOPIC + ". Waiting for product events...");

            while (true) {
                ConsumerRecords<String, Product> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, Product> record : records) {
                    Product product = record.value();
                    if (product != null) {
                        System.out.printf("Consumed Product Event: ID=%s, Name='%s', Price=%.2f, Stock=%d%n",
                                product.id(), product.name(), product.price(), product.stockQuantity());
                    } else {
                        System.err.println("Consumed null record (deserialization failure or null value).");
                    }
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (Exception e) {
            System.err.println("Consumer interrupted or encountered an error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private Properties getProperties() {
        Properties properties = Config.commonProperties();
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return properties;
    }
}
