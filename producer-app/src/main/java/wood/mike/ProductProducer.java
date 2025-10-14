package wood.mike;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import wood.mike.util.Config;

import java.util.Properties;
import java.util.UUID;

import static wood.mike.util.Config.PRODUCT_EVENTS_TOPIC;

public class ProductProducer {
    public static void main(String[] args) {
        System.out.println("Starting Kafka Product Producer...");

        Properties props = getProperties();

        try (KafkaProducer<String, Product> producer = new KafkaProducer<>(props)) {

            Product product1 = new Product(UUID.randomUUID().toString(), "Gaming Mouse", 49.99, 150);
            Product product2 = new Product(UUID.randomUUID().toString(), "Mechanical Keyboard", 129.50, 80);
            Product product3 = new Product(UUID.randomUUID().toString(), "4K Monitor", 349.99, 25);

            sendProduct(producer, product1);
            sendProduct(producer, product2);
            sendProduct(producer, product3);

            producer.flush();
            System.out.println("Finished sending all product events.");

        } catch (Exception e) {
            System.err.println("An error occurred during producer operations: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void sendProduct(KafkaProducer<String, Product> producer, Product product) {
        // Use the product ID as the key for partitioning consistency
        ProducerRecord<String, Product> record = new ProducerRecord<>(PRODUCT_EVENTS_TOPIC, product.id(), product);

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.printf("Sent Product: ID=%s, Name='%s', Partition=%d, Offset=%d%n",
                        product.id(), product.name(), metadata.partition(), metadata.offset());
            } else {
                System.err.println("Error sending product: " + exception.getMessage());
                exception.printStackTrace();
            }
        });
    }

    private static Properties getProperties() {
        Properties properties = Config.commonProperties();
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        return properties;
    }
}
