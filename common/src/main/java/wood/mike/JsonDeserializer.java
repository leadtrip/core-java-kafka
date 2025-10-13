package wood.mike;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.errors.SerializationException;
import java.util.Map;

/**
 * Custom Kafka Deserializer to convert a JSON byte array back into a Java object.
 * This class is located in the 'common' module for reuse.
 */
public class JsonDeserializer<T> implements Deserializer<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Class<T> targetClass;

    // This constructor is used by Kafka reflection when the config points to this class.
    // However, since we need to know the target Class<T> (i.e., Product.class),
    // we must manually pass the class type, which we will handle in the consumer.
    @SuppressWarnings("unchecked")
    public JsonDeserializer() {
        // Dummy initialization to satisfy Kafka's reflection requirement.
        this.targetClass = (Class<T>) Object.class;
    }

    // Required constructor for manual instantiation (used in the consumer)
    public JsonDeserializer(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No custom configuration needed
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null || targetClass == null) {
            return null;
        }
        try {
            // Convert JSON byte array back to the target Java class (T)
            return objectMapper.readValue(data, targetClass);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing JSON to " + targetClass.getName(), e);
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}
