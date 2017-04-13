package bigdata.training;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

/**
 * Created by Maksym_Panchenko on 4/11/2017.
 */
public class HomeWork1Runner {
    static {
        System.setProperty("log.timestamp", "RUNNER");
    }

    private static final Logger LOG = Logger.getLogger(HomeWork1Runner.class);

    public static void main(String[] args) throws Exception {
        String[] params = {
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
        for (String param : params) {
            LOG.info("Running first task with params: " + param);
            startSecondJVM(param);
        }

    }

    private static void startSecondJVM(String params) throws Exception {
        String separator = System.getProperty("file.separator");
        String classpath = System.getProperty("java.class.path");
        String path = System.getProperty("java.home") + separator + "bin" + separator + "java";
        final File tmp = File.createTempFile("out", null);
        final File tmpErr = File.createTempFile("out", null);
        try {
            tmp.deleteOnExit();
            tmpErr.deleteOnExit();

            ProcessBuilder processBuilder =
                    new ProcessBuilder(path, "-cp", classpath, HomeWork1Web.class.getName(), params)
                            .redirectError(tmpErr)
                            .redirectOutput(tmp);

            Process process = processBuilder.start();

            int exitCode = process.waitFor();
            final StringBuilder out = new StringBuilder();
            try (final InputStream is = new FileInputStream(tmpErr)) {
                int c;
                while ((c = is.read()) != -1) {
                    out.append((char) c);
                }
            }
            LOG.info("Task " + (exitCode == 0 ? "SUCCEED" : "FAILED") + "! Time spent: " + out.toString().trim() + " seconds");
        } finally {
            tmp.delete();
            tmpErr.delete();
        }
    }
}
