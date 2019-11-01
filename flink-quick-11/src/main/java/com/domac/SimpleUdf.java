package com.domac;

import com.domac.udf.HashCode;
import com.domac.udf.Split;
import com.domac.udf.TrojanQuery;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

public class SimpleUdf {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        //普通的hash udf
        tableEnv.registerFunction("hashCode", new HashCode(10));
        String sql = "select name, hashCode(name) from (VALUES ('Jack Ma'), ('Pony Ma'), ('Mac li')) as MyCustomTable(name)";
        Table result = tableEnv.sqlQuery(sql);
        tableEnv.toDataSet(result, Row.class).print();

        System.out.println("----------------------");

        tableEnv.registerFunction("split", new Split("_"));
        sql = "SELECT name, word, length FROM (VALUES ('Jack_Ma'),('Pony_Ma'),('Mac_li')) as MyCustomTable(name), LATERAL TABLE(split(name)) as T(word, length)";
        result = tableEnv.sqlQuery(sql);
        tableEnv.toDataSet(result, Row.class).print();

        System.out.println("----------------------");

        tableEnv.registerFunction("trojanQuery", new TrojanQuery());
        sql = "select mid, trojanQuery(mid) from (VALUES ('0096431a7a2c7313eb6272ac32f04639'), ('008fba2b987174a9b80e47bce8904335'), ('Mac-li'),('008fba2b987174a9b80e47bce8904335'),('008fba2b987174a9b80e47bce8904335')) as MyCustomTable(mid)";
        result = tableEnv.sqlQuery(sql);
        tableEnv.toDataSet(result, Row.class).print();


    }
}
