package com.domac.udf;

import com.alibaba.fastjson.JSON;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.table.functions.ScalarFunction;

import java.io.Serializable;
import java.util.List;

public class TrojanQuery extends ScalarFunction {

    public static class Result implements Serializable {

        @JsonIgnoreProperties("SafeLevel")
        int SafeLevel;

        public Result() {
        }

        public Result(int safeLevel) {
            SafeLevel = safeLevel;
        }

        public int getSafeLevel() {
            return SafeLevel;
        }

        public void setSafeLevel(int safeLevel) {
            SafeLevel = safeLevel;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "SafeLevel=" + SafeLevel +
                    '}';
        }
    }


    public TrojanQuery() {
    }

    public int eval(String mid) {
        HttpClient httpClient = MyHttpClient.getInstance();
        httpClient.getParams().setSoTimeout(200);
        PostMethod postMethod = new PostMethod("http://localhost:10029/query?md5=" + mid);
        try {
            int code = httpClient.executeMethod(postMethod);
            if (code == 200) {
                String text = postMethod.getResponseBodyAsString();
                List<Result> res = JSON.parseArray(text, Result.class);
                if (res.size() > 0) {
                    return res.get(0).SafeLevel;
                }
            }
        } catch (Exception e) {
            //e.printStackTrace();
        }
        return -1;
    }
}
