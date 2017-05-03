package bigdata.training;

/**
 * Created by Maksym_Panchenko on 5/3/2017.
 */
public class FibonacciProducerService implements Runnable {

    private FibonacciKafkaProducer producer;
    private Integer numbersToProduce = 0;

    FibonacciProducerService() {
    }

    public FibonacciProducerService(Integer numbersToProduce, final FibonacciKafkaProducer fibonacciKafkaProducer) {
        if (numbersToProduce == null || fibonacciKafkaProducer == null) {
            throw new IllegalArgumentException("numbersToProduce and fibonacciKafkaProducer should be provided!");
        }
        this.numbersToProduce = numbersToProduce;
        producer = fibonacciKafkaProducer;
    }

    public long fibonacciFn(int n) {
        if (n <= 1) return n;
        else return fibonacciFn(n - 1) + fibonacciFn(n - 2);
    }

    public void run() {
        for (int i = 1; i <= numbersToProduce; i++) {
            producer.send(fibonacciFn(i));
            System.out.println("Send #:" + i + "\t:\t" + fibonacciFn(i));
        }
        producer.close();
    }

}
