package bigdata.training;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * Created by Maksym_Panchenko on 4/16/2017.
 */
public class WebHdfsDataProviderImpl implements HdfsDataProvider {
    private static final Logger LOG = Logger.getLogger(WebHdfsDataProviderImpl.class);

    private String server = "sandbox.hortonworks.com";
    private Integer port = 50070;

    public WebHdfsDataProviderImpl(final String server, final Integer port) {
        this.server = server;
        this.port = port;
    }

    @Override
    public ReadableByteChannel getReadableByteChannel(String hdfsFilePath) throws IOException {
        Path pt = new Path(hdfsFilePath);
        FileSystem fs = null;
        try {
            URI uri = new URI("webhdfs://" + server + ":" + port);
            LOG.info("Connecting to HDFS with URI: " + uri.toString());
            fs = FileSystem.get(uri, new Configuration());
            FSDataInputStream open = fs.open(pt);
            return Channels.newChannel(open);
        } catch (Exception e) {
            LOG.error("Unhandled Exception: ", e);
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
        return null;
    }
}
