package bigdata.training;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by Maksym_Panchenko on 5/3/2017.
 */

@RunWith(MockitoJUnitRunner.class)
public class FibonacciProducerServiceTest {

    private static final int FIBONACCI_NUMBER_TO_FIND = 11;
    private static final long ELEVENTH_FIBONACCI_NUMBER = 89L;

    FibonacciProducerService sut;

    @Mock
    FibonacciKafkaProducer fibonacciKafkaProducer;

    @Before
    public void setUp() throws Exception {
        sut = new FibonacciProducerService(FIBONACCI_NUMBER_TO_FIND, fibonacciKafkaProducer);
    }

    @Test
    public void shouldProduceFibonacciNumber() throws Exception {

        long fibonacciNumber = sut.fibonacciFn(FIBONACCI_NUMBER_TO_FIND);

        assertThat(fibonacciNumber, is(ELEVENTH_FIBONACCI_NUMBER));
    }

    @Test
    public void shouldSuccessfullyRun() throws Exception {
        when(fibonacciKafkaProducer.send(anyLong())).thenReturn(null);

        sut.run();

        verify(fibonacciKafkaProducer, times(FIBONACCI_NUMBER_TO_FIND)).send(anyLong());
    }

}