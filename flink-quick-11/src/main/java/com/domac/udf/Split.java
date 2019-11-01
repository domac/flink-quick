package com.domac.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableFunction;

public class Split extends TableFunction<Tuple2<String, Integer>> {

    private String sep = "";

    public Split(String sep) {
        this.sep = sep;
    }

    public void eval(String str) {
        for (String s : str.split(sep)) {
            collect(new Tuple2<String, Integer>(s, s.length()));
        }
    }
}
