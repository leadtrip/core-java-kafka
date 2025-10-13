package wood.mike;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;

/**
 * Custom Kafka Serializer to convert any Java object (like a Product record)
 * into a JSON byte array for transmission over Kafka.
 * * This uses the Jackson library, which is automatically included via the
 * 'kafka-clients' dependency we added in the Gradle files.
 */
public class JsonSerializer<T> implements Serializer<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    // Default constructor is required by Kafka for reflection
    public JsonSerializer() {}

    // Required override methods from the Serializer interface
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Can be used to configure the Jackson ObjectMapper, but we'll use defaults.
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null) {
            return null;
        }
        try {
            // Convert the Java object (T) to a JSON byte array
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing object to JSON: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        // Nothing to close for this implementation
    }
}
