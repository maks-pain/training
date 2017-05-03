package bigdata.training;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by Maksym_Panchenko on 5/3/2017.
 */

@RunWith(MockitoJUnitRunner.class)
public class FibonacciSumConsumerServiceTest {

    private static final int FIBONACCI_NUMBER_TO_CONSUME = 11;
    private static final long SUM_OF_FIBONACCI_NUMBERS = 110L;
    private static final String TOPIC = "topic";

    FibonacciSumConsumerService sut;

    @Mock
    private KafkaConsumer<String, String> consumer;

    @Before
    public void setUp() throws Exception {
        sut = new FibonacciSumConsumerService(TOPIC, FIBONACCI_NUMBER_TO_CONSUME, consumer);
    }

    @Test
    public void run() throws Exception {
        Map map = new HashMap<TopicPartition, List<ConsumerRecord<String, String>>>() {{
            List value = singletonList(new ConsumerRecord<String, String>(TOPIC, 1, 1L, "key", "10"));
            TopicPartition key = new TopicPartition(TOPIC, 1);
            put(key, value);
        }};
        ConsumerRecords<String, String> records = new ConsumerRecords<String, String>(map);

        when(consumer.poll(anyInt())).thenReturn(records);

        sut.run();

        verify(consumer,times(12)).poll(anyLong());
    }

}