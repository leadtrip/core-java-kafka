package wood.mike;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.List;

import static wood.mike.util.Config.BOOTSTRAP_SERVERS;

/**
 * Utility class that uses the Kafka Admin API to fetch and display metadata about the brokers, cluster, and topics/partitions.
 */
public class ClusterMetadataFetcher {

    public static void main(String[] args) {
        System.out.println("Starting Kafka Cluster Metadata Fetcher...");

        // 1. Configure AdminClient Properties
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        // Use a short timeout for quicker failure if Kafka isn't running
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 3000);

        try (AdminClient adminClient = AdminClient.create(props)) {

            System.out.println("\n=============================================");
            System.out.println("           KAFKA CLUSTER METADATA");
            System.out.println("=============================================");

            // Describe Cluster and Brokers
            DescribeClusterResult clusterResult = adminClient.describeCluster();

            // Fetch Cluster ID and Controller Node
            String clusterId = clusterResult.clusterId().get();
            Node controller = clusterResult.controller().get();

            System.out.printf("Cluster ID: %s%n", clusterId);
            System.out.printf("Controller Node: %s (Host: %s, Port: %d)%n",
                    controller.id(), controller.host(), controller.port());

            // Fetch All Broker Nodes
            System.out.println("\n--- Broker Nodes ---");
            for (Node node : clusterResult.nodes().get()) {
                System.out.printf("  Node ID: %d, Host: %s, Port: %d, IsController: %b%n",
                        node.id(), node.host(), node.port(), node.id() == controller.id());
            }

            // List and Describe Topics
            System.out.println("\n=============================================");
            System.out.println("            TOPIC & PARTITION INFO");
            System.out.println("=============================================");

            // Get the list of all topic names
            Set<String> topicNames = adminClient.listTopics().names().get();

            if (topicNames.isEmpty()) {
                System.out.println("No topics found in the cluster.");
                return;
            }

            // Fetch detailed descriptions for all topics
            DescribeTopicsResult topicsResult = adminClient.describeTopics(topicNames);
            Map<String, TopicDescription> topicDescriptions = topicsResult.allTopicNames().get();;

            for (String topicName : topicNames) {
                TopicDescription desc = topicDescriptions.get(topicName);
                if (desc == null || desc.isInternal()) continue; // Skip internal topics

                System.out.printf("\nTopic: %s%n", topicName);
                System.out.printf("  Partitions: %d%n", desc.partitions().size());

                for (TopicPartitionInfo pInfo : desc.partitions()) {
                    // Leader is the node responsible for writes/reads
                    Node leader = pInfo.leader();
                    // Replicas are all nodes that hold a copy of the partition
                    List<Node> replicas = pInfo.replicas();
                    // ISR (In-Sync Replicas) are the healthy replicas
                    List<Node> isr = pInfo.isr();

                    System.out.printf("    Partition %d: Leader=%s, Replicas=%d, ISR=%d%n",
                            pInfo.partition(),
                            leader != null ? leader.idString() : "NONE",
                            replicas.size(),
                            isr.size());
                }
            }

        } catch (ExecutionException e) {
            System.err.println("Failed to fetch metadata. Is Kafka running on " + BOOTSTRAP_SERVERS + "?");
            System.err.println("Root Cause: " + e.getCause().getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Metadata fetch interrupted.");
        } catch (Exception e) {
            System.err.println("An unexpected error occurred: " + e.getMessage());
        }
    }
}
