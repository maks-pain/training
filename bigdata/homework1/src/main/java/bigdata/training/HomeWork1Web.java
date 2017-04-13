package bigdata.training;

import org.apache.log4j.Logger;

import java.net.URL;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Maksym_Panchenko on 4/4/2017.
 */
public class HomeWork1Web {
    static {
        System.setProperty("log.timestamp", new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()));
    }

    private static final Logger LOG = Logger.getLogger(HomeWork1Web.class);

    private static Map<String, Long> resultMap = new HashMap<>();

    private final static BidDataAnalyticService bidDataAnalyticService = new BidDataAnalyticService(resultMap);

    public static void main(String[] args) throws Exception {

        String server = "http://sandbox.hortonworks.com:50070";
        String openOp = "?op=OPEN";
        String homeworkDir = "/webhdfs/v1/user/maria_dev/hw1data/bid_data/";
        String[] files = {
//                "bid.20130606.txt",
//                "bid.20130607.txt",
//                "bid.20130608.txt",
//                "bid.20130609.txt",
//                "bid.20130610.txt",
//                "bid.20130611.txt",
                "bid.20130612.txt",
        };

        LOG.info("Processing start time: " + LocalTime.now());
        long startTime = System.nanoTime();

        for (String file : files) {
            bidDataAnalyticService.processFile(new URL(server + homeworkDir + file + openOp));
        }

        long stopTime2 = System.nanoTime();

        LOG.info("Processing stop time: " + LocalTime.now());
        long duration = (stopTime2 - startTime) / 1000_000_000;
        LOG.info("Processing duration time sec: " + duration);
        System.err.println(duration);

    }

}
