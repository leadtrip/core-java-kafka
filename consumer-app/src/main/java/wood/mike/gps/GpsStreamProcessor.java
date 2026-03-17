package wood.mike.gps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static wood.mike.SerdeUtils.buildJsonSerde;
import static wood.mike.util.Config.*;
import static wood.mike.util.KafkaHelper.ensureTopicsExists;

public class GpsStreamProcessor {

    private static final List<String> REQUIRED_TOPICS = Arrays.asList(GPS_RAW_TOPIC, SEGMENTS_TOPIC, ACTIVE_RUNS);

    public static void main(String[] args) {
        new GpsStreamProcessor().run();
    }

    public void run() {
        Properties props = getProperties();
        ensureTopicsExists(props, REQUIRED_TOPICS);
        final KafkaStreams streams = new KafkaStreams(buildTopology(), props);

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

    private Topology buildTopology() {
        var gpsSerde = buildJsonSerde(GpsPoint.class);
        var segmentSerde = buildJsonSerde(Segment.class);
        var stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();

        // store of start and end lat/lon for each segment
        var materialized = Materialized.<String, Segment, KeyValueStore<Bytes, byte[]>>as(SEGMENTS_STORE)
                .withKeySerde(Serdes.String())
                .withValueSerde(segmentSerde);

        builder.globalTable(
                SEGMENTS_TOPIC,
                Consumed.with(Serdes.String(), segmentSerde),
                materialized
        );

        // Temporary memory to store active runs - Key: UserID_SegmentID | Value: StartTime (Long)
        StoreBuilder<KeyValueStore<String, Long>> activeRunsStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(ACTIVE_RUNS),
                stringSerde,
                Serdes.Long()
        );
        builder.addStateStore(activeRunsStore);

        builder.stream(GPS_RAW_TOPIC, Consumed.with(stringSerde, gpsSerde))
                .process(SegmentTrackerProcessor::new, ACTIVE_RUNS);


        return builder.build();
    }

    private Properties getProperties() {
        Properties properties = commonProperties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "gps-analytics-app-v1");
        return properties;
    }
}
