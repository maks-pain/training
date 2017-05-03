package bigdata.training;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class FibonacciSumConsumerApp {

    public static void main(String[] args) throws Exception {
        String topic;
        int n;
        if (args.length != 2) {
            System.out.println("Usage: consumer <topic> <number>");
            System.out.println("[!] Fallback to default values!");
            topic = "homework";
            n = 20;
        } else {
            topic = args[0].trim();
            n = Integer.parseInt(args[1]);
        }

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("enable.auto.commit", "true");
        props.put("group.id", "0");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        FibonacciSumConsumerService consumerService = new FibonacciSumConsumerService(topic, n, consumer);

        consumerService.run();

    }


}