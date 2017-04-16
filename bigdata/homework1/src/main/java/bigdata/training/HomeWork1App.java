package bigdata.training;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.ListIterator;
import java.util.Map;

/**
 * Created by Maksym_Panchenko on 4/4/2017.
 */
public class HomeWork1App {

    static {
        System.setProperty("log.timestamp", new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()));
    }

    private static final Logger LOG = Logger.getLogger(HomeWork1App.class);

    private final static HdfsDataProvider WEB_HDFS_DATA_PROVIDER = new WebHdfsDataProviderImpl("sandbox.hortonworks.com", 50070);
    private final static HdfsDataProvider REST_HDFS_DATA_PROVIDER = new RestHdfsDataProviderImpl("sandbox.hortonworks.com", 50070);
    private static final String DEFAULT_WORKING_DIRECTORY = "/user/maria_dev/hw1data/bid_data/";
    private static final String[] DEFAULT_FILES = new String[]{"bid.20130606.txt", "bid.20130607.txt", "bid.20130608.txt", "bid.20130609.txt",
            "bid.20130610.txt", "bid.20130611.txt", "bid.20130612.txt",};

    private static Map<String, Long> resultMap = new HashMap<>();
    private static BidDataAnalyticService bidDataAnalyticService;

    public static void main(String[] args) throws Exception {

        Options options = prepareOptions();

        CommandLineParser parser = new ExtendedPosixParser(true);
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            LOG.info(e.getMessage());
            formatter.printHelp("Home work 1 options", options);
            System.exit(1);
            return;
        }

        String type = cmd.getOptionValue("type");
        String inputDir = cmd.getOptionValue("dir");
        String[] inputFiles = cmd.getOptionValues("f");
        String outputFilePath = cmd.getOptionValue("output");

        String homeworkDir = inputDir != null && inputDir.trim().length() > 0 ? inputDir : DEFAULT_WORKING_DIRECTORY;
        String[] files = inputDir != null && inputDir.length() > 0 ? inputFiles : DEFAULT_FILES;

        bidDataAnalyticService = new BidDataAnalyticService(getHdfsDataProviderByType(type), resultMap);

        LOG.info("Starting task with args: -t" + type + " -d " + homeworkDir + " -f " + Arrays.asList(files) + " -o " + outputFilePath);
        LOG.info("Processing start time: " + LocalTime.now());
        long startTime = System.nanoTime();

        for (String file : files) {
            bidDataAnalyticService.processFile(homeworkDir + file);
        }

        long stopTime2 = System.nanoTime();

        LOG.info("Processing stop time: " + LocalTime.now());
        long duration = (stopTime2 - startTime) / 1000_000_000;
        LOG.info("Processing duration time sec: " + duration);

        System.err.println(duration); //communicate with Runner via stderr

    }

    private static HdfsDataProvider getHdfsDataProviderByType(final String type) {
        if (type.equalsIgnoreCase("web")) {
            return WEB_HDFS_DATA_PROVIDER;
        } else if (type.equalsIgnoreCase("rest")) {
            return REST_HDFS_DATA_PROVIDER;
        }
        LOG.error("Unrecognized HDFS provider type! Only WEB and REST acceptable.");
        return null;
    }

    private static Options prepareOptions() {
        Options options = new Options();

        Option type = new Option("t", "type", true, "hdfs connection type");
        type.setRequired(true);
        options.addOption(type);

        Option dir = new Option("d", "dir", true, "hdfs directory path");
        dir.setRequired(false);
        options.addOption(dir);

        Option input = new Option("f", "files", true, "input files names");
        input.setRequired(false);
        input.setValueSeparator(',');
        options.addOption(input);

        Option output = new Option("o", "output", true, "output file");
        output.setRequired(false);
        options.addOption(output);
        return options;
    }

    private static class ExtendedPosixParser extends PosixParser {

        private boolean ignoreUnrecognizedOption;

        public ExtendedPosixParser(final boolean ignoreUnrecognizedOption) {
            this.ignoreUnrecognizedOption = ignoreUnrecognizedOption;
        }

        @Override
        protected void processOption(final String arg, final ListIterator iter) throws ParseException {
            boolean hasOption = getOptions().hasOption(arg);

            if (hasOption || !ignoreUnrecognizedOption) {
                super.processOption(arg, iter);
            }
        }
    }

}
