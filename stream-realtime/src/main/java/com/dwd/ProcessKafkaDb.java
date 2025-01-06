package com.dwd;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.func.DwdProcessFunction;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class ProcessKafkaDb {

    private static final String kafka_topic_db = ConfigUtils.getString("kafka.topic.db");
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String mysql_databases_conf_tb = ConfigUtils.getString("mysql.databases.conf.tb");


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);


        MySqlSource<String> mySQLDbMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );

        MySqlSource<String> mySQLCdcDWDConfSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"),
                mysql_databases_conf_tb,
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );


        DataStreamSource<String> cdcDbMainStream = env.fromSource(mySQLDbMainCdcSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_main_source");
        DataStreamSource<String> cdcDbDwdStream = env.fromSource(mySQLCdcDWDConfSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_dwd_source");


        SingleOutputStreamOperator<JSONObject> cdcDbDwdStreamMap = cdcDbDwdStream.map(JSONObject::parseObject)
                .uid("dwd_data_convert_json")
                .name("dwd_data_convert_json");
//        cdcDbDwdStreamMap.print();

        SingleOutputStreamOperator<JSONObject> cdcDbDwdStreamMapCleanColumn = cdcDbDwdStreamMap.map(s -> {
                    s.remove("source");
                    s.remove("transaction");
                    JSONObject resJson = new JSONObject();
                    if ("d".equals(s.getString("op"))){
                        resJson.put("before",s.getJSONObject("before"));
                    }else {
                        resJson.put("after",s.getJSONObject("after"));
                    }
                    resJson.put("op",s.getString("op"));
                    return resJson;
                }).uid("clean_json_column_map")
                .name("clean_json_column_map");

//        cdcDbDwdStreamMapCleanColumn.print();

        //清洗数据
        SingleOutputStreamOperator<JSONObject> flatMap = cdcDbMainStream.flatMap(new RichFlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    if (jsonObject != null) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });


        MapStateDescriptor<String, JSONObject> mapStageDesc = new MapStateDescriptor<>("mapStageDesc", String.class, JSONObject.class);
        BroadcastStream<JSONObject> broadcastDs = cdcDbDwdStreamMapCleanColumn.broadcast(mapStageDesc);
        BroadcastConnectedStream<JSONObject, JSONObject> connectDs = flatMap.connect(broadcastDs);
        connectDs.process(new DwdProcessFunction(mapStageDesc));


        env.execute();

    }
}
