package com.util;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.util.CreateTopicDbToKafka
 * @Author hou.dz
 * @Date 2025/1/2 16:09
 * @description:
 */
public class CreateTopicDbToKafka {
    public static TableResult getKafkaTopicDb(StreamTableEnvironment tenv, String topicName, String kafka_servers, String dwd_topicName) {
        return tenv.executeSql("CREATE TABLE topic_db (\n" +
                "  `before` MAP<STRING,STRING>,\n" +
                "  `after` Map<String,String>,\n" +
                "  `source` MAP<STRING,STRING>,\n" +
                "  `op` STRING,\n" +
                "  `ts_ms` BIGINT,\n" +
                "   proc_time as proctime(),\n" +
                "  row_time as TO_TIMESTAMP_LTZ(ts_ms,3) ,\n" +
                "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                ")  WITH (" +
                "    'connector' = 'kafka'," +
                "    'topic' = '"+topicName+" '," +
                "    'properties.bootstrap.servers' = ' "+kafka_servers+" '," +
                "    'properties.group.id' = '"+dwd_topicName+"', " +
                "    'scan.startup.mode' = 'earliest-offset'," +
                "    'format' = 'json' " +
                " )");
    }
}
