package wood.mike;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import wood.mike.util.Config;

import java.util.Properties;
import static wood.mike.util.Config.SIMPLE_TOPIC;

public class SimpleProducer {

    public static void main(String[] args) {
        Properties properties = getProperties();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

            for (int i = 0; i < 10; i++) {
                String key = "id_" + i;
                String value = "ST-" + i;

                ProducerRecord<String, String> record = new ProducerRecord<>(SIMPLE_TOPIC, key, value);

                producer.send(record, (metadata, e) -> {
                    if (e != null) {
                        System.err.println("Error while producing: " + e.getMessage());
                    } else {
                        System.out.println("Produced record to topic: " + metadata.topic()
                                + ", partition: " + metadata.partition()
                                + ", offset: " + metadata.offset());
                    }
                });

                Thread.sleep(100);
            }

            producer.flush();
            System.out.println("Producer finished sending messages.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static Properties getProperties() {
        Properties properties = Config.commonProperties();
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}