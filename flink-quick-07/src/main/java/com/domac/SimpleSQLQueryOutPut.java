package com.domac;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;

public interface SimpleSQLQueryOutPut {


    public static void main(String[] args) throws Exception {

        /**
         * input.txt
         *
         * domac,25
         * mark,30
         * jack,15
         */

        String input = null;
        String output = null;

        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            input = parameterTool.get("input");
            output = parameterTool.get("output");
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (input == null) {
            input = "/tmp/input_student.txt";
        }

        if (output == null) {
            output = "/tmp/output_student.csv";
        }


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        DataSource<String> dataSource = env.readTextFile(input);
        DataSet<Student> inputData = dataSource.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                String[] splits = value.split(",");
                return new Student(splits[0], Integer.parseInt(splits[1]));
            }
        });

        Table students = tableEnv.fromDataSet(inputData);
        tableEnv.registerTable("student", students);

        String sql = "select count(1), avg(age) from student";
        Table queryResult = tableEnv.sqlQuery(sql);

        CsvTableSink csvTableSink = new CsvTableSink(output, ",", 1, FileSystem.WriteMode.OVERWRITE);
        tableEnv.registerTableSink("csvOutputTable",
                new String[]{"count", "avg_age"},
                new TypeInformation[]{Types.LONG, Types.INT}, csvTableSink);

        queryResult.insertInto("csvOutputTable");

        env.execute();
    }


    public static class Student {
        public String name;
        public int age;

        public Student() {
        }

        public Student(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return "Student => {" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }
    }
}
