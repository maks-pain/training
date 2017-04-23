package bigdata.training;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.EnumMap;
import java.util.Map;

/**
 * Created by Maksym_Panchenko on 4/11/2017.
 */
public class HomeWork1Runner {
    static {
        System.setProperty("log.timestamp", "RUNNER");
    }

    private static final Logger LOG = Logger.getLogger(HomeWork1Runner.class);

    public static void main(String[] args) throws Exception {

        String workingDirectory = "/user/maria_dev/hw1data/bid_data/";
        String[] files = {
                "bid.20130606.txt",
                "bid.20130607.txt",
                "bid.20130608.txt",
                "bid.20130609.txt",
                "bid.20130610.txt",
                "bid.20130611.txt",
                "bid.20130612.txt",
        };

        String[] javaParams = {
                "-Xms256m -Xmx512m",
                "-Xms512m -Xmx1g",
                "-Xms1g -Xmx2g",
                "-Xms2g -Xmx4g",
                "-Xms2g -Xmx4g -XX:PermSize=16M -XX:MaxPermSize=32M",
                "-Xms2g -Xmx4g -XX:PermSize=64M -XX:MaxPermSize=256M",
                "-Xms2g -Xmx4g -XX:+UseG1GC -XX:ConcGCThreads",
                "-Xms2g -Xmx4g -XX:+UseStringDeduplication -XX:SurvivorRatio=2",
                "-Xms8g -Xmx8g -Xmn2g",
        };

        Map<ConnectionType, String[]> runConfig = new EnumMap<ConnectionType, String[]>(ConnectionType.class) {{
            put(ConnectionType.WEB, javaParams);
            put(ConnectionType.REST, javaParams);
        }};

        runConfig.entrySet().forEach(entry -> {
            for (String params : entry.getValue()) {
                LOG.info("Running " + entry.getKey() + " task with params: " + params);
                startSecondJVM(params, entry.getKey(), workingDirectory, files);
            }
        });
    }

    private static void startSecondJVM(final String params, ConnectionType type, String workingDirectory, final String[] files) {
        String separator = System.getProperty("file.separator");
        String classpath = System.getProperty("java.class.path");
        String path = System.getProperty("java.home") + separator + "bin" + separator + "java";

        File tmp = null;
        File tmpErr = null;
        try {
            tmp = File.createTempFile("out", null);
            tmpErr = File.createTempFile("outErr", null);
            tmp.deleteOnExit();
            tmpErr.deleteOnExit();

            ProcessBuilder processBuilder = new ProcessBuilder(
                    path, "-cp", classpath,
                    HomeWork1App.class.getName(), params,
                    "-t", type.name().toLowerCase(), "-d", workingDirectory, "-f", String.join(",", files), "-o", "abc"
            )
                    .redirectError(tmpErr)
                    .redirectOutput(tmp);

            LOG.trace(processBuilder.command());
            Process process = processBuilder.start();

            int exitCode = process.waitFor();
            StringBuilder out = getProcessResult(tmpErr);
            LOG.info("Task " + (exitCode == 0 ? "SUCCEED" : "FAILED") + "! Time spent: " + out.toString().trim() + " seconds");
        } catch (IOException | InterruptedException e) {
            LOG.error("Exception occurred while running task ", e);
        } finally {
            if (tmp != null) tmp.delete();
            if (tmpErr != null) tmpErr.delete();
        }
    }

    private static StringBuilder getProcessResult(final File tmpErr) throws IOException {
        final StringBuilder out = new StringBuilder();
        try (final InputStream is = new FileInputStream(tmpErr)) {
            int c;
            while ((c = is.read()) != -1) {
                out.append((char) c);
            }
        }
        return out;
    }

    private enum ConnectionType {
        WEB, REST
    }

}
