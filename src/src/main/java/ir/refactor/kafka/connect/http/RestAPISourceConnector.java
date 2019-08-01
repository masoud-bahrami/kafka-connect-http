package ir.refactor.kafka.connect.http;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RestAPISourceConnector extends SourceConnector {
    public static void main(String[] args) throws InterruptedException, IOException {
        System.out.println("Hello World!");

        RestAPISourceTask task = new RestAPISourceTask();
        Map<String, String> props = new HashMap<String, String>();
        props.put(RestAPISourceConnector.RSS_URI, "http://localhost:37836/api/EventFeeder/feed/");
        props.put(RestAPISourceConnector.TOPIC_CONFIG, "test");
        props.put(RestAPISourceConnector.TAKE_CONFIG, "5");
        task.start(props);

        task.poll();
        task.poll();
        task.poll();
    }

    public static final String TOPIC_CONFIG = "topic";
    public static final String RSS_URI = "rssUri";
    public static final String TAKE_CONFIG = "take";
    private String rssUri;
    private String topic;

    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> configProps) {
        rssUri = configProps.get(RSS_URI);
        topic = configProps.get(TOPIC_CONFIG);
        if (topic == null || topic.isEmpty())
            throw new ConnectException("FileStreamSourceConnector configuration must include 'topic' setting");
        if (topic.contains(","))
            throw new ConnectException(
                    "FileStreamSourceConnector should only have a single topic when used as a source.");
    }

    @Override
    public void stop() {

    }

    // defines the class that should be instantiated in worker processes
    // to actually read the data:
    @Override
    public Class<? extends Task> taskClass() {
        return RestAPISourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {

        ArrayList<Map<String, String>> configs = new ArrayList<Map<String, String>>();
        // Only one input stream makes sense.
        Map<String, String> config = new HashMap<String, String>();
        if (rssUri != null)
            config.put(RSS_URI, rssUri);
        config.put(TOPIC_CONFIG, topic);
        configs.add(config);
        return configs;
    }

    @Override
    public ConfigDef config() {
        return null;
    }
}
