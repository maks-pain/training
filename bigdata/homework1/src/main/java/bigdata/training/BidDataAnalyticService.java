package bigdata.training;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Created by Maksym_Panchenko on 4/11/2017.
 */
public class BidDataAnalyticService {

    private static final Logger LOG = Logger.getLogger(BidDataAnalyticService.class);
    private static final Comparator<Map.Entry<String, Long>> REVERSED_ENTRY_COMPARATOR = (o1, o2) -> o2.getValue().compareTo(o1.getValue());
    private static final int TOP_100 = 100;
    private static final int SAMPLE_TOP_10 = 10;

    private boolean islogSamples;
    private Long logEveryNLine;
    private Map<String, Long> resultMap;

    public BidDataAnalyticService(Map<String, Long> resultDstMap) {
        this(false, 100_000L, resultDstMap);
    }

    public BidDataAnalyticService(final boolean islogSamples, final Long logEveryNLine, final Map<String, Long> resultMap) {
        this.islogSamples = islogSamples;
        this.logEveryNLine = logEveryNLine;
        this.resultMap = resultMap;
    }

    public void processFile(final URL urlToFile) throws MalformedURLException {
        LOG.info("Start working on " + urlToFile.toString());
        try (ReadableByteChannel in = Channels.newChannel(urlToFile.openStream())) {
            ByteBuffer buf = ByteBuffer.allocate(8192);
            List<String> data = new ArrayList<>();
            StringBuffer sb = new StringBuffer();

            int linesCount = 0;
            int bytesRead = 0;

            while (bytesRead >= 0) {
                bytesRead = prepareByteBuffer(in, buf);

                while (buf.hasRemaining()) {
                    char ch = (char) buf.get();
                    sb.append(ch);
                    // check EOL
                    if (!String.valueOf(ch).matches(".")) {
                        linesCount = processLineAtEOL(sb, linesCount);
                        sb = new StringBuffer();
                    }
                }
            }

            LOG.info("Stats after file '" + urlToFile.toString() + "'");
            LOG.info("\t|- Lines processed: " + linesCount);
            resultMap.entrySet().stream()
                    .sorted(REVERSED_ENTRY_COMPARATOR)
                    .limit(TOP_100)
                    .forEach(stringLongEntry -> LOG.info("\t|- " + stringLongEntry.getKey() + ":\t" + stringLongEntry.getValue()));

        } catch (Exception ex) {
            LOG.error("Operation terminated abnormally, with exception: ", ex);
        }
    }

    private int processLineAtEOL(final StringBuffer sb, int linesCount) {
        String line = sb.toString();
        String[] columns = line.split("\t");

        if (columns.length != 21) LOG.warn("Number of columns is not 21! Actual value is " + columns.length);

        String iPinYouId = columns[2];
        // update counter for iPinYouId
        Long count = resultMap.putIfAbsent(iPinYouId, 1L);
        if (count != null) {
            resultMap.put(iPinYouId, count + 1);
        }

        logSampleInfo(++linesCount, line);

        return linesCount;
    }

    private int prepareByteBuffer(final ReadableByteChannel in, final ByteBuffer buf) throws IOException {
        int bytesRead;
        buf.rewind();
        bytesRead = in.read(buf);
        //limit is set to current position and position is set to zero
        buf.rewind();
        return bytesRead;
    }

    private void logSampleInfo(final int linesCount, final String line) {
        if (islogSamples && linesCount % logEveryNLine == 0) {
            LOG.debug("Lines count: " + linesCount);
            LOG.debug("Line sample:\n" + line);
            LOG.debug("Stats: ");
            resultMap.entrySet().stream()
                    .sorted(REVERSED_ENTRY_COMPARATOR)
                    .limit(SAMPLE_TOP_10)
                    .forEach(stringLongEntry -> LOG.debug("\t|- " + stringLongEntry.getKey() + ":\t" + stringLongEntry.getValue()));
        }
    }

}
