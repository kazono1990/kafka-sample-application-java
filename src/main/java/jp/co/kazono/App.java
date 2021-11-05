package jp.co.kazono;

import jp.co.kazono.kafka.Consumer;
import jp.co.kazono.kafka.KafkaProperties;
import jp.co.kazono.kafka.Producer;
import jp.co.kazono.kafka.Utils;

import org.apache.kafka.common.errors.TimeoutException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class App {

    public static void main(String[] args) throws InterruptedException, TimeoutException {
        String brokers = KafkaProperties.KAFKA_BROKER + ":" + KafkaProperties.KAFKA_BROKER_PORT;
        CountDownLatch countDownLatch = new CountDownLatch(2);

        Utils utils = new Utils(brokers);
        utils.createTopic(KafkaProperties.TOPIC, KafkaProperties.PARTITION, KafkaProperties.REPLICATION_FACTOR);

        Producer producer = new Producer(brokers,
            KafkaProperties.TOPIC,
            10,
            countDownLatch);
        producer.start();

        Consumer consumer = new Consumer(brokers,
            KafkaProperties.TOPIC,
            KafkaProperties.CONSUMER_GROUP_ID,
            10,
            countDownLatch);
        consumer.start();

        if (!countDownLatch.await(1, TimeUnit.MINUTES)) {
            throw new TimeoutException("Timeout after 5 minutes waiting for demo producer and consumer to finish");
        }
        consumer.shutdown();
    }
}
