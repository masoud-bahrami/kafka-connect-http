//-----------------------------------------------------------------------
// <copyright file="RestAPISourceTask.java" Project="ir.refactor.kafka.connect.rest.http Project">
//     Copyright (C) Author <Masoud Bahrami>. <http://www.Refactor.Ir>
//     Github repo <https://github.com/masoud-bahrami/kafka-connect-http>
// </copyright>
//-----------------------------------------------------------------------
package ir.refactor.kafka.connect.http;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONArray;
import org.json.JSONObject;

public class RestAPISourceTask extends SourceTask {
    private String topic;
    private URI rssURL;
    private Integer take;
    private Integer from;
    private String SOURCE = "";
    private Long last_execution = 0L;
    private Boolean hasNext = false;

    private String version = "1.0.0-snapshot";

    public String version() {
        return version;
    }

    @Override
    public void start(Map<String, String> props) {
        Logger.logInfo("Start RestAPISourceTask.start .....");

        topic = props.get(RestAPISourceConnector.TOPIC_CONFIG);
        Logger.logInfo("At RestAPISourceTask.start topic = " + topic);
        if (topic == null) {
            Logger.logInfo("At RestAPISourceTask.start topic null exception");
            throw new ConnectException("FileStreamSourceTask config missing topic setting");
        }

        Logger.logInfo("At RestAPISourceTask.start calling createConfigFileIfNotExist()");
        ConfigService.createConfigFileIfNotExist(configFileName());

        try {
            Logger.logInfo("At RestAPISourceTask.start calling getLastFeedPoint()");
            from = ConfigService.getLastFeedPoint(configFileName());
            Logger.logInfo("At RestAPISourceTask.start from = " + from);
        } catch (Exception ex) {
            from=0;
            Logger.logInfo("At RestAPISourceTask.start calling  getLastFeedPoint() got an error. ".concat(ex.getMessage()) );
        }

        try {
            rssURL = new URI(props.get(RestAPISourceConnector.RSS_URI));
            Logger.logInfo("At RestAPISourceTask.start rssURL = " + rssURL);
        } catch (URISyntaxException e) {
            Logger.logInfo("At RestAPISourceTask.start error occurred in creating new URI " + e.getMessage());
        }

        if (rssURL == null) {
            Logger.logInfo("At RestAPISourceTask.start rssURL null exception");
            throw new RuntimeException("At RestAPISourceTask.start rssURL null exception");
        }

        try {
            take = Integer.parseInt(props.get(RestAPISourceConnector.TAKE_CONFIG));
            Logger.logInfo("At RestAPISourceTask.start. getting take from props. take = " + take);

            if (take == 0)
                take = 20;
        } catch (Exception ex) {
            take = 20;
        }

        Logger.logInfo("End RestAPISourceTask.start.");
    }

    private void updateLastExecution() {
        last_execution = System.currentTimeMillis();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        updateLastExecution();
        Logger.logInfo("Start RestAPISourceTask.poll .....");
        Logger.logInfo("At RestAPISourceTask.poll calling initialFromIndex");
        initialFromIndex();

        if (!hasNext && !tellMeIfThereAreAnyNewEvents()) {
            return Collections.EMPTY_LIST;
        }

        HttpResponse response = null;
        try {
            Logger.logInfo("At RestAPISourceTask.poll. Start calling HttpClientUtility.get() with  url parameter = " + eventFeedUrl());
            response = HttpClientUtility.get(eventFeedUrl());
            Logger.logInfo("At RestAPISourceTask.poll. End executing httpRequest. Response is " + response);
        } catch (Exception e) {
            Logger.logInfo("At RestAPISourceTask.poll. Executing http request got  Exception " + e.getMessage());
        }

        try {
            Logger.logInfo("At RestAPISourceTask.poll. Start convert response message to JSONOBJECT");
            JSONObject eventsJsonObject = new JSONObject(EntityUtils.toString(response.getEntity()));
            Logger.logInfo("At RestAPISourceTask.poll. End convert response message to JSONOBJECT");

            hasNext = eventsJsonObject.getBoolean(HalEventResponseConsts.HAS_NEXT_PAGE_TITLE);
            Logger.logInfo("At RestAPISourceTask.poll. hasNext " + hasNext);

            Logger.logInfo("At RestAPISourceTask.poll. Start extracting _embedded from JSONOBJECT");
            JSONArray events = eventsJsonObject.getJSONObject(HalEventResponseConsts.EMBEDDED_PORTION_TITLE).getJSONArray(HalEventResponseConsts.EMBEDDED_EVENTS_TITLE);
            Logger.logInfo("At RestAPISourceTask.poll. End extracting _embedded from JSONOBJECT");

            List<SourceRecord> records;
            if (events != null) {
                Logger.logInfo("At RestAPISourceTask.poll. Response contains " + events.length() + " Events ");
                records = new ArrayList<SourceRecord>(events.length());

                Logger.logInfo("At RestAPISourceTask.poll. Start creating SourceRecord array");

                for (int i = 0; i < events.length(); i++) {
                    Logger.logInfo("At RestAPISourceTask.poll. Start Creating a new eventObject");
                    Object eventObject = events.get(i);
                    Logger.logInfo("At RestAPISourceTask.poll. Start calling createSourceRecord() with parameter ".concat(eventObject.toString()));
                    SourceRecord sourceRecord = createSourceRecord(eventObject, topic);

                    records.add(sourceRecord);
                }
                Logger.logInfo("At RestAPISourceTask.poll. End creating SourceRecord array");

                if (hasNext) {
                    Integer newFrom = Integer.parseInt(eventsJsonObject.getString(HalEventResponseConsts.NEXT_FROM_TITLE));
                    Integer newTake = Integer.parseInt(eventsJsonObject.getString(HalEventResponseConsts.NEXT_TAKE_TITLE));

                    Logger.logInfo("At RestAPISourceTask.poll. Start calling  storeLastFedPoint() newFrom = " + newFrom + " newTake=" + newTake);
                    ConfigService.storeLastFedPoint(configFileName(), newFrom);
                    Logger.logInfo("At RestAPISourceTask.poll. End calling  storeLastFedPoint()");
                } else {
                    Integer totalCount = Integer.parseInt(eventsJsonObject.getString(HalEventResponseConsts.TOTAL_EVENTS_TITLE));
                    Logger.logInfo("At RestAPISourceTask.poll. Start calling  storeLastFedPoint() newFrom = " + totalCount + " newTake=" + totalCount);
                    ConfigService.storeLastFedPoint(configFileName(), totalCount);
                    Logger.logInfo("At RestAPISourceTask.poll. End calling  storeLastFedPoint()");
                }

                return records;
            }
        } catch (Exception e) {
            Logger.logInfo("At RestAPISourceTask.poll. Unhandled Exception occurred !!!" + e.getMessage());
        }
        return Collections.EMPTY_LIST;
    }

    private SourceRecord createSourceRecord(Object eventObject, String topic) {
        Logger.logInfo("Start RestAPISourceTask.createSourceRecord...");
        Map<String, Object> sourcePartition = new HashMap<String, Object>();
        sourcePartition.put("importFrom", "eventFeeder");
        sourcePartition.put("source", SOURCE);

        Map<String, Object> offset = new HashMap<String, Object>();
        offset.put("last_execution", last_execution);

        Logger.logInfo("At RestAPISourceTask.createSourceRecord. Calling giveMeBinaryFormatOf() with parameter = ".concat(eventObject.toString()));

        byte[] eventObjectBytes = giveMeBinaryFormatOf(eventObject);
        Logger.logInfo("At RestAPISourceTask.createSourceRecord. Create a new SourceRecord");

        return new SourceRecord(sourcePartition,
                                offset,
                                topic,
                                Schema.BYTES_SCHEMA,
                                eventObjectBytes);
    }

    private byte[] giveMeBinaryFormatOf(Object object) {
        Logger.logInfo("Start RestAPISourceTask.giveMeBinaryFormatOf...");

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        byte[] eventObjectBytes = null;
        try {
            ObjectOutput objectOutput = new ObjectOutputStream(outputStream);
            objectOutput.writeObject(object);
            objectOutput.flush();

            eventObjectBytes = outputStream.toByteArray();
        } catch (Exception e) {
            Logger.logInfo("At RestAPISourceTask.giveMeBinaryFormatOf. Exception occured in converting object to byte[]".concat(e.getMessage()));
        } finally {
            try {
                outputStream.close();
            } catch (Exception ex) {
                // ignore close exception
            }
        }
        return eventObjectBytes;
    }

    private boolean tellMeIfThereAreAnyNewEvents() {
        Logger.logInfo("Start RestAPISourceTask.tellMeIfThereAreAnyNewEvents....");

        HttpResponse rsp = null;
        try {
            Logger.logInfo("At RestAPISourceTask.tellMeIfThereAreAnyNewEvents. Calling HttpClientUtility.Get");

            rsp = HttpClientUtility.get(nextPageOFEventUrl());
        } catch (Exception e) {
            Logger.logInfo("At RestAPISourceTask.poll. Executing http got Exception " + e.getMessage());
        }

        try {
            String str = EntityUtils.toString(rsp.getEntity());
            Logger.logInfo("At RestAPISourceTask.poll. Start deserializing response body to JsonObject. Response body is = {" + str + " }");

            JSONObject jObj = new JSONObject(str);
            Logger.logInfo("At RestAPISourceTask.poll. End deserializing response body to JsonObject. JsonObject is " + jObj);

            hasNext = jObj.getBoolean("hasNextPage");
        } catch (Exception e) {
            Logger.logInfo("At RestAPISourceTask.tellMeIfThereAreAnyNewEvents. Exception " + e.getMessage());
        }

        if (!hasNext) {
            Logger.logInfo("At RestAPISourceTask.tellMeIfThereAreAnyNewEvents. There is no any events return empty list");
        }

        Logger.logInfo("End RestAPISourceTask.tellMeIfThereAreAnyNewEvents");
        return hasNext;
    }

    private String nextPageOFEventUrl() {
        return rssURL + "/nextPage/" + from.toString();
    }

    private String eventFeedUrl() {
        return rssURL + "/" + from.toString() + "/" + take.toString();
    }

    private void initialFromIndex() {
        try {
            Logger.logInfo("Start RestAPISourceTask.initialFrom. Getting from  by calling getLastFeedPoint()");
            from = ConfigService.getLastFeedPoint(configFileName());
        } catch (Exception e1) {
            Logger.logInfo("At RestAPISourceTask.initialFrom. An error occurred in calling getLastFeedPoint()" + e1.getMessage());
            Logger.logInfo("At RestAPISourceTask.initialFrom. Set from to 0");
            from = 0;
        }
    }

    private String configFileName() {
        Logger.logInfo("Start RestAPISourceTask.configFileName() ...");

        Logger.logInfo("Start RestAPISourceTask.configFileName(). Topic is ".concat(topic));
        String fileName = topic.concat("config");
        Logger.logInfo("Start RestAPISourceTask.configFileName(). File name is ".concat(fileName));
        return fileName;
    }

    @Override
    public void stop() {
    }
}