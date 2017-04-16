package bigdata.training;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

/**
 * Created by Maksym_Panchenko on 4/16/2017.
 */
public interface HdfsDataProvider {

    ReadableByteChannel getReadableByteChannel(String hdfsFilePath) throws IOException;
}
