package wood.mike.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Config {

    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String APPLICATION_ID = "core-java-kafka";
    public static final String CATEGORY_PREFIX = "CAT-";

    public static final String SIMPLE_TOPIC = "simple-topic";
    public static final String PRODUCT_EVENTS_TOPIC = "product-events";
    public static final String LOW_STOCK_RAW_TOPIC = "low-stock-raw";
    public static final String PRODUCT_METADATA_TOPIC = "product-metadata";
    public static final String ENRICHED_LOW_STOCK_ALERTS_TOPIC = "enriched-low-stock-alerts";
    public static final String GPS_RAW_TOPIC = "gps-raw-data";
    public static final String SEGMENTS_TOPIC = "segments";
    public static final String SEGMENTS_STORE = "segments-store";
    public static final String ACTIVE_RUNS = "active-runs";
    public static final String COMPLETED_SEGMENTS_TOPIC = "completed-segments";
    public static final String GLOBAL_LEADERBOARD_STORE = "global-leaderboard-store";

    private Config() {}

    public static Properties commonProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-java-consumer-group");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }
}
