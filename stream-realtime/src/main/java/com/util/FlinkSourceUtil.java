package com.util;

import com.stream.common.utils.ConfigUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @Package com.util.FlinkSourceUtil
 * @Author hou.dz
 * @Date 2025/1/3 8:59
 * @description:
 */
public class FlinkSourceUtil {


    private final static String KAFKA_BOOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");
    private final static String MYSQL_HOST = ConfigUtils.getString("mysql.host");
    private final static String MYSQL_PORT = ConfigUtils.getString("mysql.port");
    private final static String PROCESS_DATABASE = ConfigUtils.getString("mysql.databases.conf");
    private final static String DORIS_USERNAME = ConfigUtils.getString("doris.username");
    private final static String MYSQL_PASSWORD = ConfigUtils.getString("mysql.pwd");



    public static KafkaSource<String> getKafkaSource(String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] bytes) throws IOException {
                        if (bytes != null) {
                            return new String(bytes, StandardCharsets.UTF_8);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                })
                .build();
    }

    /**
     * 读取CDC
     * @param processDataBase
     * @param processDimTableName
     * @return
     */

    public static MySqlSource<String> getMysqlSource(String processDataBase, String processDimTableName) {
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        return MySqlSource.<String>builder()
                .jdbcProperties(props)
                .hostname(MYSQL_HOST)
                .port(Integer.parseInt(MYSQL_PORT))
                .databaseList(PROCESS_DATABASE) // monitor all tables under inventory database
                .username(DORIS_USERNAME)
                .password(MYSQL_PASSWORD)
                .tableList(processDataBase + "." + processDimTableName)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to String
                .startupOptions(StartupOptions.initial())
                .build();

    }
}