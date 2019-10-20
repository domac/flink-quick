package com.domac;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;


public class SimpleSQLQuery {

    public static void main(String[] args) throws Exception {

        String input = null;

        /**
         *
         * csv 数据格式
         *
         * 排名,球员,球队,进球
         * 1,aaa,fc_1,20
         * 2,aab,fc_1,18
         * 3,bbb,fc_2,17
         * 4,ccc,fc_3,16
         * 5,bba,fc_2,15
         * 6,bbc,fc_2,14
         * 7,ddd,fc_4,9
         */

        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            input = parameterTool.get("input");
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (input == null) {
            input = "/tmp/input.csv";
        }

        //设置执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        BatchTableEnvironment tableEnv = BatchTableEnvironment.getTableEnvironment(env);

        //设置DataSet
        DataSet<Player> csvInput = env.readCsvFile(input).ignoreFirstLine()
                .pojoType(Player.class, "rank", "name", "club", "goals");

        Table players = tableEnv.fromDataSet(csvInput);

        //注册table
        tableEnv.registerTable("players", players);

        String querySQL = "select club, sum(goals) as total_goals from players group by club";
        Table queryResult = tableEnv.sqlQuery(querySQL);

        DataSet<Result> result = tableEnv.toDataSet(queryResult, Result.class);

        //算子
        result.map(new MapFunction<Result, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Result result) throws Exception {
                return Tuple2.of(result.club, result.total_goals);
            }
        }).print(); //控制台输出

    }


    public static class Player {
        public int rank;
        public String name;
        public String club;
        public int goals;

        public Player() {
            super();
        }
    }

    public static class Result {
        public String club;
        public int total_goals;

        public Result() {
        }
    }
}
