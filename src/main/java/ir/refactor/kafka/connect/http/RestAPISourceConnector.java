package ir.refactor.kafka.connect.http;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.logging.Logger;

public class RestAPISourceConnector extends SourceConnector {
    private static Logger LOGGER = Logger.getLogger("RestAPISourceConnector.InfoLogging");

    public static void main(String[] args) {
        LOGGER.info("Hello World!");

        RestAPISourceTask task = new RestAPISourceTask();
        Map<String, String> props = new HashMap<String, String>();
        props.put(RestAPISourceConnector.RSS_URI, "http://192.168.0.116:37836/api/EventFeeder/feed/");
        props.put(RestAPISourceConnector.TOPIC_CONFIG, "test");
        props.put(RestAPISourceConnector.TAKE_CONFIG, "5");
        task.start(props);

        try {
            task.poll();
            task.poll();
            task.poll();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static final String TOPIC_CONFIG = "topic";
    public static final String RSS_URI = "rssUri";
    public static final String TAKE_CONFIG = "take";
    private String rssUri;
    private String topic;

    public String version() {
        return "1.0.0";
    }

    @Override
    public void start(Map<String, String> configProps) {
        LOGGER.info("Start RestAPISourceConnector.start .....");

        rssUri = configProps.get(RSS_URI);
        LOGGER.info("At Connector.start rssUri = " + rssUri);
        topic = configProps.get(TOPIC_CONFIG);
        LOGGER.info("At Connector.start topic = " + topic);
        if (topic == null || topic.isEmpty())
        {
            LOGGER.info("FileStreamSourceConnector configuration must include 'topic' setting" );

            throw new ConnectException("FileStreamSourceConnector configuration must include 'topic' setting");
        }
        if (topic.contains(","))
        {
            LOGGER.info("FileStreamSourceConnector should only have a single topic when used as a source.");

            throw new ConnectException(
                    "FileStreamSourceConnector should only have a single topic when used as a source.");
        }
        LOGGER.info("End RestAPISourceConnector.start");

    }

    @Override
    public void stop() {
    }

    // defines the class that should be instantiated in worker processes
    // to actually read the data:
    @Override
    public Class<? extends Task> taskClass() {
        LOGGER.info("Start RestAPISourceConnector.taskClass");
        return RestAPISourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        LOGGER.info("Start RestAPISourceConnector.taskConfigs ......");
        ArrayList<Map<String, String>> configs = new ArrayList<Map<String, String>>();
        // Only one input stream makes sense.
        Map<String, String> config = new HashMap<String, String>();
        if (rssUri != null)
        {
            LOGGER.info("At RestAPISourceConnector.taskConfigs. Adding rssUri to config. rssUri = "  + rssUri);
            config.put(RSS_URI, rssUri);
        }
        LOGGER.info("At RestAPISourceConnector.taskConfigs. Adding topic to config. topic = " + topic);
        config.put(TOPIC_CONFIG, topic);
        LOGGER.info("At RestAPISourceConnector.taskConfigs. Adding config to configs");
        configs.add(config);
        LOGGER.info("End RestAPISourceConnector.taskConfigs");
        return configs;
    }

    private static final String HTTP_API_URL_DOC = "HTTP API URL.";
    public static final String CONNECTION_GROUP = "Connection";
    private static final String HTTP_API_URL_DISPLAY = "HTTP URL";

    private static final int TAKE_CONFIG_DEFAULT = 10;
    private static final ConfigDef.Range NON_NEGATIVE_INT_VALIDATOR = ConfigDef.Range.atLeast(0);
    private static final String TAKE_CONFIG_DOC = "The count of events take every time.";
    private static final String TAKE_CONFIG_GROUP = "TakeConfigs";
    private static final String TAKE_CONFIG_DISPLAY = "Take Config";

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                // Connection
                .define(RSS_URI, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        HTTP_API_URL_DOC, CONNECTION_GROUP, 1, ConfigDef.Width.LONG, HTTP_API_URL_DISPLAY)

                // Retries
                .define(TAKE_CONFIG, ConfigDef.Type.INT, TAKE_CONFIG_DEFAULT, NON_NEGATIVE_INT_VALIDATOR,
                        ConfigDef.Importance.MEDIUM, TAKE_CONFIG_DOC, TAKE_CONFIG_GROUP, 1, ConfigDef.Width.SHORT,
                        TAKE_CONFIG_DISPLAY)

                // topic
                .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                        HTTP_API_URL_DOC, CONNECTION_GROUP, 1, ConfigDef.Width.LONG, HTTP_API_URL_DISPLAY);
    }
}