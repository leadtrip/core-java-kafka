package wood.mike;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public final class SerdeUtils {

    private SerdeUtils() {}

    public static <T> Serde<T> buildJsonSerde(Class<T> clazz) {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(clazz));
    }
}
