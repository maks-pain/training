package bigdata.training;

import java.util.Properties;

public class FibonacciProducerApp {

    public static void main(String[] args) {

        String topicName;
        int n;
        if (args.length != 2) {
            System.out.println("Usage:\n" +
                    "\n\t<topic> - topic name;" +
                    "\n\t<n>     - number of Fibonacci's numbers to produce");
            System.out.println("[!] Fallback to default values!");
            topicName = "homework";
            n = 44;
        } else {
            topicName = args[0].trim();
            n = Integer.parseInt(args[1]);
        }

        System.out.println("Going to produce " + n + " Fibonacci numbers to topic: " + topicName);

        FibonacciKafkaProducer producer = new FibonacciKafkaProducer(topicName, getProperties());
        FibonacciProducerService fibonacciProducerService = new FibonacciProducerService(n, producer);

        fibonacciProducerService.run();

    }

    private static Properties getProperties() {
        // create instance for properties to access producer configs
        Properties props = new Properties();
        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");
        //Set acknowledgements for producer requests.
        props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        //Specify buffer size in config
//        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

}