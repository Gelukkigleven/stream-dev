package com.catalog.hbase;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.utils.HiveCatalogUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @Package com.dwd.CreateKafkaDimDDLCatalog
 * @Author hou.dz
 * @Date 2025/1/2 11:18
 * @description:
 */
public class CreateKafkaDimDDLCatalog {
    private static final String kafka_bootstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String topic_db = ConfigUtils.getString("kafka.topic.db");

    private static final String CreateKafkaDimDDL="CREATE TABLE topic_db (\n" +
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
            "    'topic' = '"+topic_db+" '," +
            "    'properties.bootstrap.servers' = ' "+kafka_bootstrap_servers+" '," +
            "    'properties.group.id' = '"+topic_db+"', " +
            "    'scan.startup.mode' = 'earliest-offset'," +
            "    'format' = 'json' " +
            " )";
    private static final String DROP_TABEL_PREFIX = "drop table if exists ";

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        HiveCatalog hiveCatalog = HiveCatalogUtils.getHiveCatalog("hive-catalog");
        tenv.registerCatalog("hive-catalog",hiveCatalog);
        tenv.useCatalog("hive-catalog");
        tenv.executeSql("show tables;").print();
        tenv.executeSql(DROP_TABEL_PREFIX + getCreateTableDDLTableName(CreateKafkaDimDDL));
        tenv.executeSql("show tables;").print();
        tenv.executeSql(CreateKafkaDimDDL).print();
        tenv.executeSql("show tables;").print();
        tenv.executeSql("select * from topic_db").print();

    }
    public static String getCreateTableDDLTableName(String createDDL){
        return createDDL.split(" ")[2].trim();
    }
}
