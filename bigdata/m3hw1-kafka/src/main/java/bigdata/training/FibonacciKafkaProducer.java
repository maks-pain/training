package bigdata.training;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * Created by Maksym_Panchenko on 5/2/2017.
 */
public class FibonacciKafkaProducer extends KafkaProducer<String, String> {

    private String topic;

    public FibonacciKafkaProducer(String topicName, final Properties props) {
        super(props);
        this.topic = topicName;
    }

    public Future<RecordMetadata> send(final Long fib) {
        return super.send(new ProducerRecord<String, String>(topic, UUID.randomUUID().toString(), fib.toString()));
    }
}
