package wood.mike.product;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Produced;
import wood.mike.JsonDeserializer;
import wood.mike.JsonSerializer;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import static wood.mike.SerdeUtils.buildJsonSerde;
import static wood.mike.util.Config.*;
import static wood.mike.util.KafkaHelper.ensureTopicsExists;

public class ProductStreamProcessor {

    private static final List<String> REQUIRED_TOPICS =
            List.of(PRODUCT_EVENTS_TOPIC,
                    PRODUCT_METADATA_TOPIC,
                    LOW_STOCK_RAW_TOPIC,
                    ENRICHED_LOW_STOCK_ALERTS_TOPIC
            );

    private static final int LOW_STOCK_THRESHOLD = 100;

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
            streams.close();
            System.exit(1);
        }
    }

    /**
     * Check for low stock products and isolate into a new topic.
     * Cross-reference/join with product metadata topic and create enriched record adding to another topic.
     * We're using a left join (as opposed to inner join) on a GlobalKTable allowing us to create an enriched record even if we don't find a matching category.
     */
    public Topology buildPipeline() {
        var productSerde = buildJsonSerde(Product.class);
        var metaSerde = buildJsonSerde(ProductMetadata.class);
        var alertSerde = buildJsonSerde(EnrichedLowStockAlert.class);
        var stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(PRODUCT_EVENTS_TOPIC, Consumed.with(stringSerde, productSerde))
                .filter((key, product) -> product.stockQuantity() < LOW_STOCK_THRESHOLD)
                .peek((k, v) -> System.out.printf("Low stock product id:%s, category:%s, quantity:%d%n", v.id(), v.categoryId(), v.stockQuantity()))
                .to(LOW_STOCK_RAW_TOPIC, Produced.with(stringSerde, productSerde));

        GlobalKTable<String, ProductMetadata> metadataTable = builder.globalTable(
                PRODUCT_METADATA_TOPIC,
                Consumed.with(stringSerde, metaSerde)
        );

        builder.stream(LOW_STOCK_RAW_TOPIC, Consumed.with(stringSerde, productSerde))
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
                .peek((key, alert) -> System.out.println(alert))
                .to(ENRICHED_LOW_STOCK_ALERTS_TOPIC, Produced.with(stringSerde, alertSerde));
        return builder.build();
    }



    private Properties getProperties() {
        Properties properties = commonProperties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "product-stream-consumer");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        return properties;
    }


}
