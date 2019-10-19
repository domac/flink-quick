package com.domac;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

public class BatchWordCountJava {

    public static void main(String[] args) throws Exception {
        String input = "";

        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            input = parameterTool.get("input");
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (null == input || input.isEmpty()) {
            input = "/tmp/batch_in";
        }

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //把文件作为输入源
        DataSource<String> text = env.readTextFile(input);

        //批处理sink
        DataSet<Tuple2<String, Long>> dataSet = text.flatMap(new BatchTokenizer());
        dataSet.print();
        //延迟执行
        //env.execute("my batch word count application");
    }


    public static class BatchTokenizer implements FlatMapFunction<String, Tuple2<String, Long>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
            String[] words = value.toLowerCase().split("\\W+");
            for (String w : words) {
                if (w.length() > 0) {
                    out.collect(new Tuple2<String, Long>(w, 1L));
                }
            }
        }
    }
}


