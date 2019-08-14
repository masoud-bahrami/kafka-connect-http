package ir.refactor.kafka.connect.http;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

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
    private static Logger LOGGER = Logger.getLogger("RestAPISourceTask.InfoLogging");
    private String topic;
    private URI rssURL;
    private Integer take;
    private Integer from;
    private String SOURCE = "";
    private Long last_execution = 0L;
    private Boolean hasNext = false;

    public String version() {
        return "1.0.0";
    }

    @Override
    public void start(Map<String, String> props) {
        LOGGER.info("Start RestAPISourceTask.start .....");
        LOGGER.info("At RestAPISourceTask.start calling createConfigFileIfNotExist()");
        createConfigFileIfNotExist();
        try {
            LOGGER.info("At RestAPISourceTask.start calling getLastFeedPoint()");
            from =getLastFeedPoint();
            LOGGER.info("At RestAPISourceTask.start from = " + from);
        } catch (Exception ex) {
             try {
                throw ex;
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        String uri = props.get(RestAPISourceConnector.RSS_URI);
        LOGGER.info("At RestAPISourceTask.start uri = " + uri);

        try {
            rssURL = new URI(uri);
            LOGGER.info("At RestAPISourceTask.start rssURL = " + rssURL);

        } catch (URISyntaxException e) {
            LOGGER.info("At RestAPISourceTask.start error occurred in creating new URI " + e.getMessage());
        }

        if (rssURL == null)
        {
            LOGGER.info("At RestAPISourceTask.start rssURL null exception" );
            throw new RuntimeException("At RestAPISourceTask.start rssURL null exception");
        }

        topic = props.get(RestAPISourceConnector.TOPIC_CONFIG);
        LOGGER.info("At RestAPISourceTask.start topic = " + topic );
        if (topic == null) {
            LOGGER.info("At RestAPISourceTask.start topic null exception" );

            throw new ConnectException("FileStreamSourceTask config missing topic setting");
        }try {

            take = Integer.parseInt(props.get(RestAPISourceConnector.TAKE_CONFIG));
            LOGGER.info("At RestAPISourceTask.start. getting take from props. take = " + take );

            if (take == 0)
                take = 20;

        } catch (Exception ex) {
            take = 20;
        }
        LOGGER.info("End RestAPISourceTask.start.");
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        last_execution = System.currentTimeMillis();
        LOGGER.info("Start RestAPISourceTask.poll .....");
        HttpClient client = HttpClientBuilder.create().build();
        try {
            LOGGER.info("At RestAPISourceTask.poll. Getting from  by calling getLastFeedPoint()");
            from =getLastFeedPoint();
        } catch (Exception e1) {
            LOGGER.info("At RestAPISourceTask.poll. An error occured in calling getLastFeedPoint()");
            LOGGER.info("At RestAPISourceTask.poll. Set from to 0");
            from = 0;
            e1.printStackTrace();
        }


        if (!hasNext) {
            LOGGER.info("At RestAPISourceTask.poll. hasNext = false");
            HttpUriRequest httpUriRequest = new HttpGet(rssURL + "/nextPage/" + from.toString());
            LOGGER.info("At RestAPISourceTask.poll. creating EventFeedUrl. " + httpUriRequest.getURI());
            // hasNextPage
            // nextFrom
            // nextTake
            // NextLink
            HttpResponse rsp = null;
            try {
                LOGGER.info("At RestAPISourceTask.poll. Executing http get request");

                rsp = client.execute(httpUriRequest);
            } catch (ClientProtocolException e) {
                LOGGER.info("At RestAPISourceTask.poll. Error in Executing http get request " + e.getMessage());
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

            LOGGER.info("At RestAPISourceTask.poll. hasNext = " + hasNext);

            if (!hasNext)
            {
                LOGGER.info("At RestAPISourceTask.poll. There is no any events return empty list");
                return Collections.EMPTY_LIST;
            }
        }
        HttpUriRequest httpUriRequest = new HttpGet(rssURL + "/" + from.toString() + "/" + take.toString());
        LOGGER.info("At RestAPISourceTask.poll. Creating fething rss url " + httpUriRequest.getURI());

        HttpResponse response = null;
        try {
            LOGGER.info("At RestAPISourceTask.poll. Start executing httpRequest to feth events");

            response = client.execute(httpUriRequest);

            LOGGER.info("At RestAPISourceTask.poll. End executing httpRequest. Response is " + response);

        } catch (ClientProtocolException e) {
            LOGGER.info("At RestAPISourceTask.poll. ClientProtocolException !! " + e.getMessage());
        } catch (IOException e) {
            LOGGER.info("At RestAPISourceTask.poll. IOException !! " + e.getMessage());
        }

        String json_string = null;
        try {
            LOGGER.info("At RestAPISourceTask.poll. Start get response body " );

            json_string = EntityUtils.toString(response.getEntity());
            LOGGER.info("At RestAPISourceTask.poll. End get response body " );
        } catch (ParseException e) {
            LOGGER.info("At RestAPISourceTask.poll. ParseException!! "  + e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            LOGGER.info("At RestAPISourceTask.poll. IOException!! "  + e.getMessage());
        }
        try {
            LOGGER.info("At RestAPISourceTask.poll. Start convert response message to JSONOBJECT");
            JSONObject eventsJsonObject = new JSONObject(json_string);
            LOGGER.info("At RestAPISourceTask.poll. End convert response message to JSONOBJECT");

            hasNext = eventsJsonObject.getBoolean("hasNextPage");
            LOGGER.info("At RestAPISourceTask.poll. hasNext " + hasNext);
            if (hasNext) {
                Integer newFrom = Integer.parseInt(eventsJsonObject.getString("nextFrom"));
                Integer newTake = Integer.parseInt(eventsJsonObject.getString("nextTake"));

                // String nextUrl = eventsJsonObject.getJSONObject("_links").getString("next");

                LOGGER.info("At RestAPISourceTask.poll. Start calling  storeLastFedPoint() newFrom = " + newFrom + " newTake=" +newTake);
                storeLastFedPoint(newFrom, newTake);
                LOGGER.info("At RestAPISourceTask.poll. End calling  storeLastFedPoint()");
            } else {

                Integer totalCount = Integer.parseInt(eventsJsonObject.getString("totalEvents"));
                LOGGER.info("At RestAPISourceTask.poll. Start calling  storeLastFedPoint() newFrom = " + totalCount + " newTake=" +totalCount);
                storeLastFedPoint(totalCount, totalCount);
                LOGGER.info("At RestAPISourceTask.poll. End calling  storeLastFedPoint()");
            }

            LOGGER.info("At RestAPISourceTask.poll. Start extracting _embedded from JSONOBJECT");
            JSONArray events = eventsJsonObject.getJSONObject("_embedded").getJSONArray("events");
            LOGGER.info("At RestAPISourceTask.poll. End extracting _embedded from JSONOBJECT");

            // String nextUrl = eventsJsonObject.getJSONObject("_links").getString("next");

            List<SourceRecord> records;
            if (events != null) {
                LOGGER.info("At RestAPISourceTask.poll. Response contains " + events.length() + " Events ");
                records = new ArrayList<SourceRecord>(events.length());

                LOGGER.info("At RestAPISourceTask.poll. Start creating SourceRecord array");

                for (int i = 0; i < events.length(); i++) {
                    LOGGER.info("At RestAPISourceTask.poll. Start Creating a new eventObject");
                    Object eventObject = events.get(i);
                    LOGGER.info("At RestAPISourceTask.poll. End Creating new eventObject " + eventObject);
                    Map<String, Object> sourcePartition = new HashMap<String, Object>();
                    sourcePartition.put("importFrom", "eventFeeder");
                    sourcePartition.put("source", SOURCE);

                    Map<String, Object> offset = new HashMap<String, Object>();
                    offset.put("last_execution", last_execution);

                    LOGGER.info("At RestAPISourceTask.poll. Create a new SourceRecord");

                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    ObjectOutput out = null;
                    byte[] eventObjectBytes;
                    try {
                        try {
                            out = new ObjectOutputStream(bos);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        try {
                            out.writeObject(eventObject);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        try {
                            out.flush();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        eventObjectBytes = bos.toByteArray();
                    } finally {
                        try {
                            bos.close();
                        } catch (IOException ex) {
                            // ignore close exception
                        }
                    }

                    SourceRecord sourceRecord = new SourceRecord(sourcePartition,
                            offset,
                            topic,
                            Schema.BYTES_SCHEMA,
                            eventObjectBytes);

                    records.add(sourceRecord);
                }
                LOGGER.info("At RestAPISourceTask.poll. End creating SourceRecord array");
                return records;
            }
        } catch (JSONException e) {
            LOGGER.info("At RestAPISourceTask.poll. JSONException " + e.getMessage());
        }
        return Collections.EMPTY_LIST;
    }

    @Override
    public void stop() {
    }

    private void storeLastFedPoint(Integer from, Integer take) {
        LOGGER.info("Starting RestAPISourceTask.storeLastFedPoint...");
        Writer out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("config"), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            LOGGER.info("At RestAPISourceTask.storeLastFedPoint. UnsupportedEncodingException =" + e.getMessage());

        } catch (FileNotFoundException e) {
            LOGGER.info("At RestAPISourceTask.storeLastFedPoint. FileNotFoundException =" + e.getMessage());
        }
        try {
            try {
                out.write(from.toString());
            } catch (IOException e) {
                LOGGER.info("At RestAPISourceTask.storeLastFedPoint. IOException =" + e.getMessage());
            }
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                LOGGER.info("At RestAPISourceTask.storeLastFedPoint. Closing config file. IOException =" + e.getMessage());

            }
        }
        LOGGER.info("End RestAPISourceTask.storeLastFedPoint...");
    }

    public Integer getLastFeedPoint() throws IOException {
        LOGGER.info("Start RestAPISourceTask.getLastFeedPoint ....");

        String content;

        byte[] bytes = Files.readAllBytes(Paths.get("config"));
        content = new String(bytes);

        // String[] result = content.split(seperator.toString());

        Integer lastFedPoint = Integer.parseInt(content);
        LOGGER.info("At RestAPISourceTask.getLastFeedPoint lastFedPoint = " + lastFedPoint);
        LOGGER.info("End RestAPISourceTask.getLastFeedPoint ....");
        return lastFedPoint;
    }

    public void createConfigFileIfNotExist() {
        LOGGER.info("Start RestAPISourceTask.createConfigFileIfNotExist ....");
        File f = new File("config");
        if (!f.exists()) {
            try {
                LOGGER.info("At RestAPISourceTask.createConfigFileIfNotExist, config file not exist. Trying to create a new config file....");
                f.createNewFile();
            } catch (IOException e) {
                LOGGER.info("At RestAPISourceTask.createConfigFileIfNotExist, creating config file failed. " + e.getMessage());

                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } else {
            LOGGER.info("At RestAPISourceTask.createConfigFileIfNotExist, Config File already exists");
        }
        LOGGER.info("End RestAPISourceTask.createConfigFileIfNotExist");

    }

}