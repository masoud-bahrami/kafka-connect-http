//-----------------------------------------------------------------------
// <copyright file="HttpClientUtility.java" Project="ir.refactor.kafka.connect.rest.http Project">
//     Copyright (C) Author <Masoud Bahrami>. <http://www.Refactor.Ir>
//     Github repo <https://github.com/masoud-bahrami/kafka-connect-http>
// </copyright>
//-----------------------------------------------------------------------
package ir.refactor.kafka.connect.http;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.kafka.common.protocol.types.Field;

import java.io.IOException;

public class HttpClientUtility {
    public static HttpResponse get(String url) throws IOException {
        Logger.logInfo("Start HttpClientUtility.get()....");
        Logger.logInfo("At HttpClientUtility.get(). url = " + url);
        HttpClient client = HttpClientBuilder.create().build();

        HttpUriRequest httpUriRequest = new HttpGet(url);
        Logger.logInfo("At HttpClientUtility.get(). Executing get request  ");
        HttpResponse rsp = client.execute(httpUriRequest);
        Logger.logInfo("At HttpClientUtility.get(). Get request  executed. Response = " + rsp);
        return rsp;
    }
}
