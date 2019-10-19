package com.domac;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.io.File;

public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        String input = "";
        String output = "";
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            input = parameterTool.get("input");
            output = parameterTool.get("output");
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (null == input || input.isEmpty()) {
            input = "/tmp/batch_in";
        }

        if (null == output || output.isEmpty()) {
            output = "/tmp/batch_out";
        }

        File file = new File(output);

        try {
            if (file.exists()) {
                if(file.delete()){
                    System.out.println(file.getName() + " 文件已被删除！");
                }else{
                    System.out.println("文件删除失败！");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //把文件作为输入源
        DataSource<String> text = env.readTextFile(input);

        //批处理sink
        DataSet<Tuple2<String, Long>> dataSet = text.flatMap(new BatchTokenizer()).groupBy(0).sum(1);
        dataSet.writeAsCsv(output, "\n", "").setParallelism(1);
        //延迟执行
        env.execute("my batch word count application");
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


