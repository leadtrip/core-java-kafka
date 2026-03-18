package wood.mike.gps;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import wood.mike.JsonSerializer;
import wood.mike.util.Config;

import java.util.List;
import java.util.Properties;

import static wood.mike.util.Config.GPS_RAW_TOPIC;

public class GpsProducer {

    public static final String USER_ID = "mikew";

    // add some other runners to simulate a race, scale the elapsed time (e.g. 0.99 for faster, 1.01 for slower)
    private static final List<OtherRunner> OTHER_ATHLETES =
            List.of(new OtherRunner("jackb", 0.99),
                    new OtherRunner("maryh", 0.98),
                    new OtherRunner("donp", 1.01),
                    new OtherRunner("chrisf", 1.02),
                    new OtherRunner("suek", 1.08));

    public static void main(String[] args) {
        new GpsProducer().run();
    }

    private void run(){
        Properties props = getProperties();

        try (KafkaProducer<String, GpsPoint> producer = new KafkaProducer<>(props)) {

            simulateEvent(GpxParser.parseGpx("morning_run_15032026.gpx", USER_ID), producer);

            producer.flush();
            System.out.println("Finished event simulation.");

        } catch (Exception e) {
            System.err.println("An error occurred during producer operations: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void simulateEvent(List<GpsPoint> points, KafkaProducer<String, GpsPoint> producer) throws InterruptedException {
        long raceStartTime = points.getFirst().timestamp();

        for (GpsPoint current : points) {
            long originalElapsed = current.timestamp() - raceStartTime;

            // Send the original runner (me, on a ~20k local run)
            producer.send(new ProducerRecord<>(GPS_RAW_TOPIC, current.userId(), current));

            // to make this a race, send some other runners off with times based off the original
            for (OtherRunner runner : OTHER_ATHLETES) {
                long scaledElapsed = (long) (originalElapsed * runner.multiplier());
                long newTimestamp = raceStartTime + scaledElapsed;

                GpsPoint orPoint = new GpsPoint(
                        runner.userId(),
                        current.lat(),
                        current.lon(),
                        current.elev(),
                        newTimestamp
                );

                producer.send(new ProducerRecord<>(GPS_RAW_TOPIC, orPoint.userId(), orPoint));
            }
            Thread.sleep(10);
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
