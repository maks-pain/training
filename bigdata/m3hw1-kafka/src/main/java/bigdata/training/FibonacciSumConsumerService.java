package bigdata.training;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;

/**
 * Created by Maksym_Panchenko on 5/3/2017.
 */
public class FibonacciSumConsumerService implements Runnable {

    private String topic;
    private int n;
    private KafkaConsumer<String, String> consumer;

    public FibonacciSumConsumerService(final String topic, final int n, final KafkaConsumer<String, String> consumer) {
        this.topic = topic;
        this.n = n;
        this.consumer = consumer;
    }

    public void run() {

        consumer.subscribe(Arrays.asList(topic));
        System.out.println("Subscribed to topic " + topic);
        long sum = 0;
        int count = 1;

        loop:
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
                if (count <= n) {
                    long fibNumber = Long.parseLong(record.value());
                    sum += fibNumber;
                    count++;
                    System.out.println("Current sum of " + count + " numbers is " + sum);
                } else {
                    break loop;
                }
            }
        }
        consumer.close();
        System.out.println("\nTotal sum of " + count + " numbers is " + sum);
    }
}
