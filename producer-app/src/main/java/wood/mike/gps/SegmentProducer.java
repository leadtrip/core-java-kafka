package wood.mike.gps;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import wood.mike.JsonSerializer;
import wood.mike.util.Config;

import java.util.List;
import java.util.Properties;

import static wood.mike.util.Config.SEGMENTS_TOPIC;

public class SegmentProducer {
    public static void main(String[] args) {
        new SegmentProducer().run();
    }

    private void run(){
        Properties props = getProperties();

        try (KafkaProducer<String, Segment> producer = new KafkaProducer<>(props)) {
            List.of(
                    new Segment("chalfield-house-to-coombe-lane", 51.368624, -2.202076, 51.378529, -2.191990),
                    new Segment("corsham-junction-to-atworth", 51.389964, -2.161956, 51.392698, -2.197052),
                    new Segment("norrington-lane-from-broughton-gifford", 51.375251, -2.172270, 51.387441, -2.167396))
                        .forEach(segment -> producer.send(new ProducerRecord<>(SEGMENTS_TOPIC, segment.segmentId(), segment)));

            producer.flush();
            System.out.println("Finished adding segments.");

        } catch (Exception e) {
            System.err.println("Error producing segments: " + e.getMessage());
            e.printStackTrace();
        }
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
