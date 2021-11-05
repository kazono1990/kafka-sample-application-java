package jp.co.kazono.kafka;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer extends ShutdownableThread {

    private KafkaConsumer consumer;
    private String topic;
    private String groupId;
    private int numMessageToConsume;
    private int messageRemaining;
    private CountDownLatch countDownLatch;

    public Consumer(String brokers,
                    String topic,
                    String groupId,
                    int numMessageToConsume,
                    CountDownLatch countDownLatch) {
        super("KafkaConsumerSample", false);
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
        this.groupId = groupId;
        this.numMessageToConsume = numMessageToConsume;
        this.messageRemaining = numMessageToConsume;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void doWork() {
        long start = System.currentTimeMillis();
        consumer.subscribe(Collections.singleton(this.topic));
        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println("Message: " + record.value());
        }
        consumer.commitAsync();
        numMessageToConsume -= records.count();
        if (numMessageToConsume <= 0) {
            System.out.println(groupId + " finished reading " + numMessageToConsume + " messages");
            consumer.close();
            countDownLatch.countDown();
            System.out.println("Processing Time[Kafka Client]: " + (System.currentTimeMillis() - start) + "[ms]");
        }
    }
}
