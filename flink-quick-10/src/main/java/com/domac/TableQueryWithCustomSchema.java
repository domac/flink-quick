package com.domac;

import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.io.Serializable;

/**
 * 自定义Schema
 */
public class TableQueryWithCustomSchema {

    public static void main(String[] args) throws Exception {

        String zk = null;
        String kafka = null;

        String inputSQL = null;

        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);

            //--zk "192.168.159.130:2181"
            zk = parameterTool.get("zk");

            //--kafka "192.168.159.130:9092"
            kafka = parameterTool.get("kafka");

            //--inputSQL "select Mid,Rawlog.OptModifyTime from agentLogs"
            inputSQL = parameterTool.get("inputSQL");
        } catch (Exception e) {
            e.printStackTrace();
        }


        if (null == zk) {
            zk = "192.168.159.130:2181";
        }

        if (null == kafka) {
            kafka = "192.168.159.130:9092";
        }

        if (null == inputSQL) {
            inputSQL = "select * from agentLogs";
        }


        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        //定义 Table Environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.connect(new Kafka().version("0.11").topic("edrlog").startFromLatest()
                .property("zookeeper.connect", zk)
                .property("bootstrap.servers", kafka))
                .withFormat(new Json().deriveSchema())
                //数据表的schema
                .withSchema(new Schema()
                        .field("Ver", Types.STRING)
                        .field("Mid", Types.STRING)
                        .field("Plugin", Types.STRING)
                        .field("Tag", Types.STRING)
                        .field("Time", Types.LONG)
                        .field("event_time", Types.SQL_TIMESTAMP).rowtime(new Rowtime() //定义时间时间
                                .timestampsFromField("event_timestamp")//event_timestamp 格式必须是 2019-10-24T22:18:30.000Z
                                .watermarksPeriodicBounded(1000))
                        .field("Rawlog", Types.POJO(RawLog.class))
                ).inAppendMode().registerTableSource("agentLogs");

        //String querySQL = "select TUMBLE_END(event_time, INTERVAL '2' HOUR) from agentLogs group by TUMBLE(event_time, INTERVAL '2' HOUR)";
        Table result = tableEnv.sqlQuery(inputSQL);

        tableEnv.toRetractStream(result, Row.class).print();

        String server_info = "zk:" + zk + "_kafka:" + kafka + "_sql=" + inputSQL;
        env.execute(server_info);
    }


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RawLog implements Serializable {

        //--------------- 基本信息 ---------------
        @Setter
        @Getter
        @JsonProperty(value = "Time")
        Long EventTime;//事件事件

        @Setter
        @Getter
        @JsonProperty(value = "Type")
        int Type;

        @Setter
        @Getter
        @JsonProperty(value = "Op")
        int Op;

        //--------------- 操作者信息 ---------------

        @Setter
        @Getter
        @JsonProperty(value = "OptPid")
        Long OptPid;

        @Setter
        @Getter
        @JsonProperty(value = "OptProcPath")
        String OptProcPath;

        @Setter
        @Getter
        @JsonProperty(value = "OptFileSize")
        int OptFileSize;

        @Setter
        @Getter
        @JsonProperty(value = "OptModifyTime")
        Long OptModifyTime;

        @Setter
        @Getter
        @JsonProperty(value = "OptProcName")
        String OptProcName;

        @Setter
        @Getter
        @JsonProperty(value = "OptCmdline")
        String OptCmdline;

        @Setter
        @Getter
        @JsonProperty(value = "OptPPid")
        Long OptPPid;

        @Setter
        @Getter
        @JsonProperty(value = "OptStdin")
        String OptStdin;

        @Setter
        @Getter
        @JsonProperty(value = "OptStdout")
        String OptStdout;

        @Setter
        @Getter
        @JsonProperty(value = "OptProcMd5")
        String OptProcMd5;

        @Setter
        @Getter
        @JsonProperty(value = "OptProcUserName")
        String OptProcUserName;

        @Setter
        @Getter
        @JsonProperty(value = "OptProcUid")
        Long OptProcUid;

        @Setter
        @Getter
        @JsonProperty(value = "OptProcGroupName")
        String OptProcGroupName;

        @Setter
        @Getter
        @JsonProperty(value = "OptProcGid")
        Long OptProcGid;

        @Setter
        @Getter
        @JsonProperty(value = "OptScanResult")
        int OptScanResult;

        //--------------- 子信息 ---------------

        @Setter
        @Getter
        @JsonProperty(value = "SubFileName")
        String SubFileName;

        @Setter
        @Getter
        @JsonProperty(value = "SubFilePath")
        String SubFilePath;

        @Setter
        @Getter
        @JsonProperty(value = "SubPid")
        Long SubPid;

        @Setter
        @Getter
        @JsonProperty(value = "SubProcCmdline")
        String SubProcCmdline;

        @Setter
        @Getter
        @JsonProperty(value = "SubUserName")
        String SubUserName;

        @Setter
        @Getter
        @JsonProperty(value = "SubUid")
        Long SubUid;

        @Setter
        @Getter
        @JsonProperty(value = "SubGroupName")
        String SubGroupName;

        @Setter
        @Getter
        @JsonProperty(value = "SubGid")
        Long SubGid;

        @Setter
        @Getter
        @JsonProperty(value = "SubProcStdin")
        String SubProcStdin;

        @Setter
        @Getter
        @JsonProperty(value = "SubProcStdout")
        String SubProcStdout;

        @Setter
        @Getter
        @JsonProperty(value = "SubFileSize")
        Long SubFileSize;

        @Setter
        @Getter
        @JsonProperty(value = "SubModifyTime")
        Long SubModifyTime;

        @Setter
        @Getter
        @JsonProperty(value = "SubFileMd5")
        String SubFileMd5;

        @Setter
        @Getter
        @JsonProperty(value = "SubFileType")
        int SubFileType;

        @Setter
        @Getter
        @JsonProperty(value = "SubScanResult")
        int SubScanResult;

        //--------------- 网络类型字段 ---------------

        @Setter
        @Getter
        @JsonProperty(value = "SubLIP")
        String SubLIP;

        @Setter
        @Getter
        @JsonProperty(value = "SubRIP")
        String SubRIP;

        @Setter
        @Getter
        @JsonProperty(value = "SubLPort")
        int SubLPort;

        @Setter
        @Getter
        @JsonProperty(value = "SubRPort")
        int SubRPort;

        @Setter
        @Getter
        @JsonProperty(value = "SubNetStatus")
        String SubNetStatus;

        @Setter
        @Getter
        @JsonProperty(value = "SubNetProto")
        String SubNetProto;

        //--------------- 日志采集信息 ---------------

        @Setter
        @Getter
        @JsonProperty(value = "LogName")
        String LogName;

        @Setter
        @Getter
        @JsonProperty(value = "LogText")
        String LogText;

        @Setter
        @Getter
        @JsonProperty(value = "LogOffset")
        Long LogOffset;

        //--------------- 环境相关信息 ---------------

        @Setter
        @Getter
        @JsonProperty(value = "Platform")
        int Platform;

        @Setter
        @Getter
        @JsonProperty(value = "OsVersion")
        String OsVersion;

        @Setter
        @Getter
        @JsonProperty(value = "Os6432")
        int Os6432;

        @Setter
        @Getter
        @JsonProperty(value = "IPList")
        String IPList;

        @Setter
        @Getter
        @JsonProperty(value = "EdrVersion")
        String EdrVersion;

        public RawLog() {
        }

        public RawLog(Long eventTime, int type, int op, Long optPid, String optProcPath, int optFileSize, Long optModifyTime, String optProcName, String optCmdline, Long optPPid, String optStdin, String optStdout, String optProcMd5, String optProcUserName, Long optProcUid, String optProcGroupName, Long optProcGid, int optScanResult, String subFileName, String subFilePath, Long subPid, String subProcCmdline, String subUserName, Long subUid, String subGroupName, Long subGid, String subProcStdin, String subProcStdout, Long subFileSize, Long subModifyTime, String subFileMd5, int subFileType, int subScanResult, String subLIP, String subRIP, int subLPort, int subRPort, String subNetStatus, String subNetProto, String logName, String logText, Long logOffset, int platform, String osVersion, int os6432, String IPList, String edrVersion) {
            EventTime = eventTime;
            Type = type;
            Op = op;
            OptPid = optPid;
            OptProcPath = optProcPath;
            OptFileSize = optFileSize;
            OptModifyTime = optModifyTime;
            OptProcName = optProcName;
            OptCmdline = optCmdline;
            OptPPid = optPPid;
            OptStdin = optStdin;
            OptStdout = optStdout;
            OptProcMd5 = optProcMd5;
            OptProcUserName = optProcUserName;
            OptProcUid = optProcUid;
            OptProcGroupName = optProcGroupName;
            OptProcGid = optProcGid;
            OptScanResult = optScanResult;
            SubFileName = subFileName;
            SubFilePath = subFilePath;
            SubPid = subPid;
            SubProcCmdline = subProcCmdline;
            SubUserName = subUserName;
            SubUid = subUid;
            SubGroupName = subGroupName;
            SubGid = subGid;
            SubProcStdin = subProcStdin;
            SubProcStdout = subProcStdout;
            SubFileSize = subFileSize;
            SubModifyTime = subModifyTime;
            SubFileMd5 = subFileMd5;
            SubFileType = subFileType;
            SubScanResult = subScanResult;
            SubLIP = subLIP;
            SubRIP = subRIP;
            SubLPort = subLPort;
            SubRPort = subRPort;
            SubNetStatus = subNetStatus;
            SubNetProto = subNetProto;
            LogName = logName;
            LogText = logText;
            LogOffset = logOffset;
            Platform = platform;
            OsVersion = osVersion;
            Os6432 = os6432;
            this.IPList = IPList;
            EdrVersion = edrVersion;
        }

        @Override
        public String toString() {
            return "RawLog{" +
                    "EventTime=" + EventTime +
                    ", Type=" + Type +
                    ", Op=" + Op +
                    ", OptPid=" + OptPid +
                    ", OptProcPath='" + OptProcPath + '\'' +
                    ", OptFileSize=" + OptFileSize +
                    ", OptModifyTime=" + OptModifyTime +
                    ", OptProcName='" + OptProcName + '\'' +
                    ", OptCmdline='" + OptCmdline + '\'' +
                    ", OptPPid=" + OptPPid +
                    ", OptStdin='" + OptStdin + '\'' +
                    ", OptStdout='" + OptStdout + '\'' +
                    ", OptProcMd5='" + OptProcMd5 + '\'' +
                    ", OptProcUserName='" + OptProcUserName + '\'' +
                    ", OptProcUid=" + OptProcUid +
                    ", OptProcGroupName='" + OptProcGroupName + '\'' +
                    ", OptProcGid=" + OptProcGid +
                    ", OptScanResult=" + OptScanResult +
                    ", SubFileName='" + SubFileName + '\'' +
                    ", SubFilePath='" + SubFilePath + '\'' +
                    ", SubPid=" + SubPid +
                    ", SubProcCmdline='" + SubProcCmdline + '\'' +
                    ", SubUserName='" + SubUserName + '\'' +
                    ", SubUid=" + SubUid +
                    ", SubGroupName='" + SubGroupName + '\'' +
                    ", SubGid=" + SubGid +
                    ", SubProcStdin='" + SubProcStdin + '\'' +
                    ", SubProcStdout='" + SubProcStdout + '\'' +
                    ", SubFileSize=" + SubFileSize +
                    ", SubModifyTime=" + SubModifyTime +
                    ", SubFileMd5='" + SubFileMd5 + '\'' +
                    ", SubFileType=" + SubFileType +
                    ", SubScanResult=" + SubScanResult +
                    ", SubLIP='" + SubLIP + '\'' +
                    ", SubRIP='" + SubRIP + '\'' +
                    ", SubLPort=" + SubLPort +
                    ", SubRPort=" + SubRPort +
                    ", SubNetStatus='" + SubNetStatus + '\'' +
                    ", SubNetProto='" + SubNetProto + '\'' +
                    ", LogName='" + LogName + '\'' +
                    ", LogText='" + LogText + '\'' +
                    ", LogOffset=" + LogOffset +
                    ", Platform=" + Platform +
                    ", OsVersion='" + OsVersion + '\'' +
                    ", Os6432=" + Os6432 +
                    ", IPList='" + IPList + '\'' +
                    ", EdrVersion='" + EdrVersion + '\'' +
                    '}';
        }
    }
}
