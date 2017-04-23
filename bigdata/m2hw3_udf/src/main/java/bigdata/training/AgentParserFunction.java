package bigdata.training;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Maksym_Panchenko on 4/22/2017.
 */
@Description(
        name = "AgentParserFunction",
        value = "returns splitted agent string in such columns 'device, os_name, browser, user_agent' all are STRING data type",
        extended = "SELECT parse_userAgent(userAgentColumn) from foo limit 1;"
)
public class AgentParserFunction extends GenericUDTF {

    private StructObjectInspector structOI;

    @Override
    public StructObjectInspector initialize(final StructObjectInspector argOIs) throws UDFArgumentException {
        if (argOIs == null) return null;
        if (argOIs.getAllStructFieldRefs().size() != 1) {
            throw new UDFArgumentException("AgentParserFunction() takes exactly one argument");
        }

        // input inspectors
        structOI = argOIs;

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("device");
        fieldNames.add("os_name");
        fieldNames.add("browser");
        fieldNames.add("user_agent");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(final Object[] objects) throws HiveException {
        List<? extends StructField> allStructFieldRefs = structOI.getAllStructFieldRefs();
        StructField structField = allStructFieldRefs.get(0);

        final String agentString = (String) structOI.getStructFieldData(objects[0], structField);

        UserAgent userAgent = UserAgent.parseUserAgentString(agentString);
        Browser browser = userAgent.getBrowser();
        OperatingSystem operatingSystem = userAgent.getOperatingSystem();

        String browserName = browser.getName();
        String ua = browser.getBrowserType().getName();
        String osName = operatingSystem.getName();
        String deviceType = operatingSystem.getDeviceType().getName();
        String[] result = {deviceType, osName, browserName, ua};
        forward(result);

    }

    @Override
    public void close() throws HiveException {

    }
}
