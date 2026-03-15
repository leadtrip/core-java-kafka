package wood.mike.product;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import wood.mike.JsonSerializer;
import wood.mike.util.Config;

import java.util.List;
import java.util.Properties;

import static wood.mike.util.Config.CATEGORY_PREFIX;
import static wood.mike.util.Config.PRODUCT_METADATA_TOPIC;

public class ProductMetadataProducer implements ProductMetadataHelper{
    public static void main(String[] args) {
        System.out.println("Starting Kafka ProductMetadata Producer...");
        new ProductMetadataProducer().run();
    }

    private void run() {
        Properties props = getProperties();

        try (KafkaProducer<String, ProductMetadata> producer = new KafkaProducer<>(props)) {

            List<ProductMetadata> productMetadataList = getProductMetadataList();
            for(int i = 0; i < productMetadataList.size(); i++) {
                sendProductMetadata(producer, productMetadataList.get(i), CATEGORY_PREFIX + i);
            }

            producer.flush();
            System.out.println("Finished sending all ProductMetadata events.");

        } catch (Exception e) {
            System.err.println("An error occurred during producer operations: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void sendProductMetadata(KafkaProducer<String, ProductMetadata> producer, ProductMetadata pm, String key) {
        ProducerRecord<String, ProductMetadata> record = new ProducerRecord<>(PRODUCT_METADATA_TOPIC, key, pm);

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                System.out.printf("Sent ProductMetadata: category=%s, supplier='%s', key=%s\n",
                        pm.category(), pm.supplier(), key);
            } else {
                System.err.println("Error sending ProductMetadata: " + exception.getMessage());
                exception.printStackTrace();
            }
        });
    }

    private Properties getProperties() {
        Properties properties = Config.commonProperties();
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return properties;
    }
}
