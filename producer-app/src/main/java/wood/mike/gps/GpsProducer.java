package wood.mike.gps;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import wood.mike.JsonSerializer;
import wood.mike.util.Config;

import java.util.List;
import java.util.Properties;

import static java.lang.Thread.sleep;
import static wood.mike.util.Config.GPS_RAW_TOPIC;

public class GpsProducer {

    public static final String USER_ID = "mikew";

    public static void main(String[] args) {
        new GpsProducer().run();
    }

    private void run(){
        Properties props = getProperties();

        try (KafkaProducer<String, GpsPoint> producer = new KafkaProducer<>(props)) {

            simulateRide(GpxParser.parseGpx("morning_run_15032026.gpx", USER_ID), producer);

            producer.flush();
            System.out.println("Finished ride simulation.");

        } catch (Exception e) {
            System.err.println("An error occurred during producer operations: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void simulateRide(List<GpsPoint> points, KafkaProducer<String, GpsPoint> producer) throws InterruptedException {
        for (int i = 0; i < points.size(); i++) {
            GpsPoint current = points.get(i);

            System.out.println("Sending: " + current);
            producer.send(new ProducerRecord<>(GPS_RAW_TOPIC, current.userId(), current));

            // uncomment to produce realistic GPS point timestamps
            /*if (i < points.size() - 1) {
                long waitTime = points.get(i + 1).timestamp() - current.timestamp();
                sleep(waitTime);
            }*/
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
