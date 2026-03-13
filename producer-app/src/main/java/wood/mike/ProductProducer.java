package wood.mike;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import wood.mike.util.Config;

import java.util.Properties;
import java.util.Random;

import static wood.mike.util.Config.CATEGORY_PREFIX;
import static wood.mike.util.Config.PRODUCT_EVENTS_TOPIC;

public class ProductProducer {

    // deliberately make the total categories larger than the number of ProductMetadata records added (50)
    // this will ensure we create products with no matching category, and we will see the left join working in the ProductStreamProcessor
    private static final int TOTAL_CATEGORIES = 100;
    private static final int TOTAL_PRODUCTS = 100;

    public static void main(String[] args) {
        System.out.println("Starting Kafka Product Producer...");
        new ProductProducer().run();
    }

    private void run() {
        Random random = new Random();

        Properties props = getProperties();

        try (KafkaProducer<String, Product> producer = new KafkaProducer<>(props)) {

            for (int i = 0; i < TOTAL_PRODUCTS; i++) {
                sendProduct(producer,
                        Product.of(
                        String.valueOf(i),
                        "product-"+i,
                        random.nextLong(400),
                        random.nextInt(200),
                        CATEGORY_PREFIX + random.nextInt(TOTAL_CATEGORIES)
                ));
                Thread.sleep(random.nextInt(1000));
            }

            producer.flush();
            System.out.println("Finished sending all product events.");

        } catch (Exception e) {
            System.err.println("An error occurred during producer operations: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void sendProduct(KafkaProducer<String, Product> producer, Product product) {
        ProducerRecord<String, Product> record = new ProducerRecord<>(PRODUCT_EVENTS_TOPIC, product.id(), product);

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.printf("Sent Product: ID=%s, Name=%s, Price=%.2f, Quantity=%d%n",
                        product.id(), product.name(), product.price(), product.stockQuantity());
            } else {
                System.err.println("Error sending product: " + exception.getMessage());
                exception.printStackTrace();
            }
        });
    }

    private Properties getProperties() {
        Properties properties = Config.commonProperties();
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);
        return properties;
    }
}
