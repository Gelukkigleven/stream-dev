package com.retailersv1.func;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.domain.TableProcessDim;
import com.retailersv1.domain.TableProcessDwd;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.JdbcUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @Package:
 * @Author:com.dz
 * @Date:2024/12/26 9:50
 * @description:
 **/
public class DwdProcessFunction extends BroadcastProcessFunction<JSONObject, JSONObject, JSONObject> {

    //实例化描述器
    private MapStateDescriptor<String,JSONObject> mapStateDescriptor;
    private HashMap<String, TableProcessDwd> configMap =  new HashMap<>();


    public DwdProcessFunction(MapStateDescriptor<String, JSONObject> mapDescriptor) {
        this.mapStateDescriptor = mapDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Connection connection = JdbcUtils.getMySQLConnection(
                ConfigUtils.getString("mysql.url"),
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"));
        String querySQL = "select * from gmall_config.table_process_dwd";
        List<TableProcessDwd> tableProcessDwds = JdbcUtils.queryList(connection, querySQL, TableProcessDwd.class, true);
        for (TableProcessDwd tableProcessDwd : tableProcessDwds ){
            configMap.put(tableProcessDwd.getSourceTable(),tableProcessDwd);
        }
    }


    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        ReadOnlyBroadcastState<String, JSONObject> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        JSONObject after = jsonObject.getJSONObject("after");
        String tableName = jsonObject.getJSONObject("source").getString("table");
        if (broadcastState!=null) {
            ArrayList<JSONObject> arr = new ArrayList<>();
            if (configMap.containsKey(tableName)){
                arr.add(after);
                KafkaUtils.sinkJson2KafkaMessage(tableName,arr);
            }
        }

    }

    @Override
    public void processBroadcastElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        BroadcastState<String, JSONObject> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String tableName = jsonObject.getJSONObject("after").getString("tableName");
        broadcastState.put(tableName,jsonObject);

    }

    @Override
    public void close() throws Exception {
        super.close();
    }

}

