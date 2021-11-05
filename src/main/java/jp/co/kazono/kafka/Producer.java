package jp.co.kazono.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class Producer extends Thread {

    private KafkaProducer<Integer, String> producer;
    private String topic;
    private int numRecordsToProduce;
    private CountDownLatch countDownLatch;

    public Producer(String brokers,
                    String topic,
                    int numRecordsToProduce,
                    CountDownLatch countDownLatch) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "SampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(props);

        this.topic = topic;
        this.numRecordsToProduce = numRecordsToProduce;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        int messageKey = 0;
        int recordsSent = 0;
        while (recordsSent < numRecordsToProduce) {
            String messageStr = "Message_" + messageKey;
            try {
                producer.send(new ProducerRecord<>(
                    topic,
                    messageKey,
                    messageStr)).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            messageKey++;
            recordsSent++;
        }
        System.out.println("Producer sent " + numRecordsToProduce + " records successfully");
        countDownLatch.countDown();
    }
}
