package com.domac.udf;

import org.apache.commons.httpclient.HttpClient;

class MyHttpClient {

    private static HttpClient instance = null;

    static HttpClient getInstance() {
        if (instance == null) {
            synchronized (HttpClient.class) {
                if (instance == null) {
                    instance = new HttpClient();
                }
            }
        }
        return instance;
    }
}


