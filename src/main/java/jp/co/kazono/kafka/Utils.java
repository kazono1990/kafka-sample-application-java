package jp.co.kazono.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Utils {
    private Logger logger = LogManager.getLogger(Utils.class);
    private AdminClient adminClient;
    private Long DELETE_TIMEOUT_SECONDS = 10l;

    public Utils (String brokers) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        adminClient = AdminClient.create(props);
    }

    public void createTopic(String topic, int partition, int replicationFactor) {
        logger.info("Creating topic { name: {}, partitions: {}, replicationFactor: {}}",
            topic, partition, replicationFactor);

        if (listTopic().contains(topic)) {
            logger.warn("Topic '{}' is already exists.", topic);
            return;
        }

        NewTopic newTopic = new NewTopic(topic, partition, (short) replicationFactor);
        CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));

        try {
            result.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private Set<String> listTopic() {
        Set<String> topics = new HashSet();
        try {
            topics = adminClient.listTopics().names().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return topics;
    }

    public void deleteTopic(String topic) {
        try {
            adminClient.deleteTopics(Collections.singleton(topic))
                .all()
                .get(DELETE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            logger.error("Did not receive delete topic response within %d seconds. Checking if it succeeded",
                DELETE_TIMEOUT_SECONDS);
            e.printStackTrace();
        }
    }
}
