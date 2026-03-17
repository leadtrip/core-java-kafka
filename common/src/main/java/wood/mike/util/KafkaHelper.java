package wood.mike.util;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.List;
import java.util.Properties;
import java.util.Set;

public class KafkaHelper {
    public static void ensureTopicsExists(Properties props, List<String> topicNames) {
        try (AdminClient adminClient = AdminClient.create(props)) {
            Set<String> existingTopics = adminClient.listTopics().names().get();

            List<NewTopic> newTopics = topicNames.stream()
                    .filter(topicName -> !existingTopics.contains(topicName))
                    .map(topicName -> {
                        System.out.println("Topic " + topicName + " missing. Creating it...");
                        return new NewTopic(topicName, 1, (short) 1);
                    }).toList();

            adminClient.createTopics(newTopics).all().get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to ensure topic exists", e);
        }
    }
}
