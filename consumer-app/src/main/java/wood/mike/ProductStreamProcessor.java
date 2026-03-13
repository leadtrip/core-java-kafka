package wood.mike;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import static wood.mike.util.Config.*;

public class ProductStreamProcessor {

    private static final List<String> REQUIRED_TOPICS =
            List.of(PRODUCT_EVENTS_TOPIC,
                    PRODUCT_METADATA_TOPIC,
                    LOW_STOCK_RAW_TOPIC,
                    ENRICHED_LOW_STOCK_ALERTS_TOPIC
            );

    public static void main(String[] args) throws Exception {
        new ProductStreamProcessor().run();
    }

    private void run() {
        final Properties props = getProperties();
        ensureTopicsExists(props, REQUIRED_TOPICS);

        final KafkaStreams streams = new KafkaStreams(buildPipeline(), props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        try {
            streams.start();
            System.out.println("Stream started successfully.");
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Check for low stock products and isolate into a new topic.
     * Cross-reference/join with product metadata topic and create enriched record adding to another topic.
     * We're using a left join allowing us to create an enriched record even if we don't find a matching category.
     */
    public Topology buildPipeline() {
        var productSerde = buildJsonSerde(Product.class);
        var metaSerde = buildJsonSerde(ProductMetadata.class);
        var alertSerde = buildJsonSerde(EnrichedLowStockAlert.class);

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(PRODUCT_EVENTS_TOPIC, Consumed.with(Serdes.String(), productSerde))
                .filter((key, product) -> product.stockQuantity() < 100)
                .peek((k, v) -> System.out.printf("Low stock product id:%s, category:%s, quantity:%d%n", v.id(), v.categoryId(), v.stockQuantity()))
                .to(LOW_STOCK_RAW_TOPIC, Produced.with(Serdes.String(), productSerde));

        GlobalKTable<String, ProductMetadata> metadataTable = builder.globalTable(
                PRODUCT_METADATA_TOPIC,
                Consumed.with(Serdes.String(), metaSerde)
        );

        builder.stream(LOW_STOCK_RAW_TOPIC, Consumed.with(Serdes.String(), productSerde))
                .leftJoin(
                    metadataTable,
                    (productId, product) -> product.categoryId(),   // the join expressed by, Product->categoryId -> (key)ProductMetadata
                    (product, meta) -> {
                        if (meta == null) {
                            return new EnrichedLowStockAlert(product.id(), product.stockQuantity(), "UNKNOWN", "UNKNOWN", System.currentTimeMillis());
                        }
                        return new EnrichedLowStockAlert(product.id(), product.stockQuantity(), meta.category(), meta.supplier(), System.currentTimeMillis());
                    }
                )
                .peek((key, alert) -> System.out.println("Enriched low stock alert: " + alert))
                .to(ENRICHED_LOW_STOCK_ALERTS_TOPIC, Produced.with(Serdes.String(), alertSerde));
        return builder.build();
    }

    private <T> Serde<T> buildJsonSerde(Class<T> clazz) {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(clazz));
    }

    private Properties getProperties() {
        Properties properties = commonProperties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        return properties;
    }

    public void ensureTopicsExists(Properties props, List<String> topicNames) {
        try (AdminClient adminClient = AdminClient.create(props)) {
            Set<String> existingTopics = adminClient.listTopics().names().get();

            List<NewTopic> newTopics = topicNames.stream()
                    .filter(topicName -> !existingTopics.contains(topicName))
                    .map(topicName -> {
                        System.out.println("Topic " + topicName + " missing. Creating it...");
                        return new NewTopic(topicName, 1, (short) 1);
                    }).toList();

            adminClient.createTopics(newTopics).all().get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to ensure topic exists", e);
        }
    }
}
