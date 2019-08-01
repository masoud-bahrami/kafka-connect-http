package ir.refactor.kafka.connect.http;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class RestAPISourceTask extends SourceTask {

    private String topic;
    private URI rssURL;
    private Integer take;
    private Integer from;
    private String SOURCE = "";
    private Long last_execution = 0L;
    private Character seperator = ',';
    private Boolean hasNext = false;

    public String version() {
        return null;
    }

    @Override
    public void start(Map<String, String> props) {
        createConfigFileIfNotExist();
        try {
            from = getLastFeedPoint();
        } catch (Exception ex) {
            // throw ex;
        }

        String uri = props.get(RestAPISourceConnector.RSS_URI);

        try {
            rssURL = new URI(uri);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }

        if (rssURL == null)
            throw new ConnectException("FileStreamSourceTask config missing topic setting");

        topic = props.get(RestAPISourceConnector.TOPIC_CONFIG);
        if (topic == null)
            throw new ConnectException("FileStreamSourceTask config missing topic setting");
        try {
            take = Integer.parseInt(props.get(RestAPISourceConnector.TAKE_CONFIG));

            if (take == 0)
                take = 20;

        } catch (Exception ex) {
            take = 20;
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        last_execution = System.currentTimeMillis();
        HttpClient client = HttpClientBuilder.create().build();
        try {
            from = getLastFeedPoint();
        } catch (Exception e1) {
            from = 0;
            e1.printStackTrace();
        }

        if (!hasNext) {
            HttpUriRequest httpUriRequest = new HttpGet(rssURL + "/nextPage/" + from.toString());

            // hasNextPage
            // nextFrom
            // nextTake
            // NextLink
            HttpResponse rsp = null;
            try {
                rsp = client.execute(httpUriRequest);
            } catch (ClientProtocolException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            String str = "";
            try {
                str = EntityUtils.toString(rsp.getEntity());
            } catch (ParseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            JSONObject jObj = null;
            try {
                jObj = new JSONObject(str);
            } catch (JSONException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            try {
                hasNext = jObj.getBoolean("hasNextPage");
            } catch (JSONException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            if (!hasNext)
                return Collections.EMPTY_LIST;
        }
        HttpUriRequest httpUriRequest = new HttpGet(rssURL + "/" + from.toString() + "/" + take.toString());

        HttpResponse response = null;
        try {
            response = client.execute(httpUriRequest);
            System.out.println(response);

        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        String json_string = null;
        try {
            json_string = EntityUtils.toString(response.getEntity());
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            JSONObject eventsJsonObject = new JSONObject(json_string);

            hasNext = eventsJsonObject.getBoolean("hasNextPage");
            if (hasNext) {
                Integer newFrom = Integer.parseInt(eventsJsonObject.getString("nextFrom"));
                Integer newTake = Integer.parseInt(eventsJsonObject.getString("nextTake"));

                // String nextUrl = eventsJsonObject.getJSONObject("_links").getString("next");

                storeLastFedPoint(newFrom, newTake);
            } else {
                Integer totalCount = Integer.parseInt(eventsJsonObject.getString("totalEvents"));
                storeLastFedPoint(totalCount, totalCount);
            }
            JSONArray events = eventsJsonObject.getJSONObject("_embedded").getJSONArray("events");

            // String nextUrl = eventsJsonObject.getJSONObject("_links").getString("next");

            List<SourceRecord> records;
            if (events != null) {
                records = new ArrayList<SourceRecord>(events.length());

                for (int i = 0; i < events.length(); i++) {
                    Object eventObject = events.get(i);

                    Map<String, Object> sourcePartition = new HashMap<String, Object>();
                    sourcePartition.put("importFrom", "eventFeeder");
                    sourcePartition.put("source", SOURCE);

                    Map<String, Object> offset = new HashMap<String, Object>();
                    offset.put("last_execution", last_execution);

                    records.add(new SourceRecord(sourcePartition, offset, topic, Schema.BYTES_SCHEMA, eventObject));
                }

                return records;
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return Collections.EMPTY_LIST;
    }

    @Override
    public void stop() {
    }

    private void storeLastFedPoint(Integer from, Integer take) {

        Writer out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("config"), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            try {
                out.write(from.toString());
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public Integer getLastFeedPoint() throws IOException {

        String content;

        byte[] bytes = Files.readAllBytes(Paths.get("config"));
        content = new String(bytes);

        // String[] result = content.split(seperator.toString());

        Integer lastFedPoint = Integer.parseInt(content);
        return lastFedPoint;
    }

    public void createConfigFileIfNotExist() {
        File f = new File("config");
        if (!f.exists()) {
            try {
                f.createNewFile();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            System.out.println("Config File already exists");
        }
    }
}