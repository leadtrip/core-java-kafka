package wood.mike.gps;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static wood.mike.util.Config.*;

class SegmentTrackerProcessor implements Processor<String, GpsPoint, String, SegmentCompletion> {
    private KeyValueStore<String, Long> runStore;
    private ReadOnlyKeyValueStore<String, ValueAndTimestamp<Segment>> segmentsStore;
    private ProcessorContext<String, SegmentCompletion> context;
    private static final DateTimeFormatter HH_MM_SS = DateTimeFormatter.ofPattern("HH:mm:ss");

    private static final double SEGMENT_PROXIMITY_THRESHOLD_METERS = 15.0;

    @Override
    public void init(ProcessorContext<String, SegmentCompletion> context) {
        this.context = context;
        this.runStore = context.getStateStore(ACTIVE_RUNS);
        this.segmentsStore = context.getStateStore(SEGMENTS_STORE);
    }

    @Override
    public void process(Record<String, GpsPoint> record) {
        String userId = record.key();
        GpsPoint point = record.value();

        // Check every segment in the GlobalKTable
        segmentsStore.all().forEachRemaining(entry -> {
            Segment seg = entry.value.value();
            String runKey = userId + "_" + seg.segmentId();

            // Check for START
            if (GpsUtils.isWithinDistance(point.lat(), point.lon(), seg.startLat(), seg.startLon(), SEGMENT_PROXIMITY_THRESHOLD_METERS)) {
                Long existingStart = runStore.get(runKey);

                // only log/record if:
                // 1. It's the first time we've seen a start (null)
                // OR
                // 2. The point we just got is EARLIER than what's in the book, covers going over a segment start the 'wrong' way
                if (existingStart == null || point.timestamp() < existingStart) {
                    runStore.put(runKey, point.timestamp());
                    System.out.println(">>> START: " + time(point.timestamp()) + " " + userId + " entered " + seg.segmentId());
                }
            }

            // Check for FINISH
            else if (GpsUtils.isWithinDistance(point.lat(), point.lon(), seg.endLat(), seg.endLon(), SEGMENT_PROXIMITY_THRESHOLD_METERS)) {
                Long startTime = runStore.get(runKey);
                if (startTime != null) {
                    long duration = point.timestamp() - startTime;
                    runStore.delete(runKey); // Clear the notebook for next time

                    // Send the result to the next topic
                    context.forward(new Record<>(seg.segmentId(), new SegmentCompletion(seg.segmentId(), userId, duration, point.timestamp()), point.timestamp()));
                    System.out.println("<<< FINISH: " + time(point.timestamp()) + " " + userId + " completed " + seg.segmentId() + " in " + (duration/1000) + "s");
                }
            }
        });
    }

    private String time(long dts) {
        return LocalTime.ofInstant(Instant.ofEpochMilli(dts), ZoneId.systemDefault()).format(HH_MM_SS);
    }
}