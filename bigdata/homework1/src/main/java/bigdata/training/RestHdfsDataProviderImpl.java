package bigdata.training;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * Created by Maksym_Panchenko on 4/16/2017.
 */
public class RestHdfsDataProviderImpl implements HdfsDataProvider {
    private static final Logger LOG = Logger.getLogger(RestHdfsDataProviderImpl.class);

    private static final String REST_PREFIX = "/webhdfs/v1";
    private static final String OPEN_OP = "?op=OPEN";

    private String server = "sandbox.hortonworks.com";
    private Integer port = 50070;

    public RestHdfsDataProviderImpl(final String server, final Integer port) {
        this.server = server;
        this.port = port;
    }

    @Override
    public ReadableByteChannel getReadableByteChannel(final String hdfsFilePath) {
        try {
            URL url = new URL("http://" + server + ":" + port + REST_PREFIX + hdfsFilePath + OPEN_OP);
            return Channels.newChannel(url.openStream());
        } catch (IOException e) {
            LOG.error("Unhandled exception: ", e);
        }

        return null;
    }
}
