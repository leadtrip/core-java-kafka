package wood.mike;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Custom Kafka Deserializer to convert a JSON byte array back into a Java object.
 */
public class JsonDeserializer<T> implements Deserializer<T> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Class<T> targetClass;


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
    public T deserialize(String topic, byte[] data) {
        if (data == null || targetClass == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, targetClass);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing JSON to " + targetClass.getName(), e);
        }
    }
}
