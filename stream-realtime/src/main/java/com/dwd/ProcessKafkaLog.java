package com.dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Package:
 * @Author:com.dz
 * @Date:2024/12/24 11:35
 * @description:
 **/
public class ProcessKafkaLog {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        KafkaSource<String> kafkaSource = KafkaUtils.buildKafkaSource(ConfigUtils.getString("kafka.bootstrap.servers"),
                ConfigUtils.getString("REALTIME.KAFKA.LOG.TOPIC"),
                "group1",
                OffsetsInitializer.earliest());

        DataStreamSource<String> kafkaLogSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_log_source");



        SingleOutputStreamOperator<JSONObject> jsonOb = kafkaLogSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {

                }
            }
        });

        KeyedStream<JSONObject, String> keyDS = jsonOb.keyBy(json -> json.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> fixedDS = keyDS.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor =
                        new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                valueState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                String isnew = jsonObject.getJSONObject("common").getString("is_new");
                String visit = valueState.value();
                Long ts = jsonObject.getLong("ts");
                String curVisitDate = DateFormatUtil.tsToDate(ts);
                if ("1".equals(isnew)) {
                    if (StringUtils.isEmpty(visit)) {
                        valueState.update(curVisitDate);
                    } else {
                        if (!valueState.equals(curVisitDate)) {
                            isnew = "0";
                            jsonObject.getJSONObject("common").put("is_new", isnew);
                        }
                    }
                } else {
                    if (StringUtils.isNoneEmpty(visit)) {
                        String yesterDay = DateFormatUtil.tsToDateTime(ts - 24 * 60 * 60 * 1000);
                        valueState.update(yesterDay);
                    }
                }
                return jsonObject;
            }
        });


        OutputTag<String> errTag = new OutputTag<String>("errTag") {};
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};

        SingleOutputStreamOperator<String> pageDS = fixedDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                JSONObject errjson = jsonObject.getJSONObject("err");
                if (errjson != null) {
                    context.output(errTag, jsonObject.toJSONString());
                }
                JSONObject startjson = jsonObject.getJSONObject("start");
                if (startjson != null) {
                    context.output(startTag, jsonObject.toJSONString());
                } else {
                    JSONObject commonjson = jsonObject.getJSONObject("common");
                    JSONObject pagejson = jsonObject.getJSONObject("page");
                    Long ts = jsonObject.getLong("ts");
                    JSONArray displayArr = jsonObject.getJSONArray("displays");
                    if (displayArr != null && displayArr.size() > 0) {
                        for (int i = 0; i < displayArr.size(); i++) {
                            JSONObject displayjson = displayArr.getJSONObject(i);
                            JSONObject newdisplay = new JSONObject();
                            newdisplay.put("common", commonjson);
                            newdisplay.put("page", pagejson);
                            newdisplay.put("ts", ts);
                            newdisplay.put("display", displayjson);
                            context.output(displayTag, newdisplay.toJSONString());
                        }
                        jsonObject.remove("displays");
                    }
                    JSONArray actionArr = jsonObject.getJSONArray("actions");
                    if (actionArr != null && actionArr.size() > 0) {
                        for (int i = 0; i < actionArr.size(); i++) {
                            JSONObject actionjson = actionArr.getJSONObject(i);
                            JSONObject newaction = new JSONObject();
                            newaction.put("common", commonjson);
                            newaction.put("page", pagejson);
                            newaction.put("action", actionjson);
                            context.output(actionTag, newaction.toJSONString());
                        }
                        jsonObject.remove("actions");
                    }
                    collector.collect(jsonObject.toJSONString());
                }
            }
        });

        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDs = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);

        pageDS.print("页面");
        errDS.print("错误");
        startDS.print("启动");
        displayDs.print("曝光");
        actionDS.print("动作");

        pageDS.sinkTo(FlinkSinkUtil.getKafkaSink("dwd_traffic_page"));
        errDS.sinkTo(FlinkSinkUtil.getKafkaSink("dwd_traffic_err"));
        startDS.sinkTo(FlinkSinkUtil.getKafkaSink("dwd_traffic_start"));
        displayDs.sinkTo(FlinkSinkUtil.getKafkaSink("dwd_traffic_display"));
        actionDS.sinkTo(FlinkSinkUtil.getKafkaSink("dwd_traffic_action"));


        env.execute();

    }
}
