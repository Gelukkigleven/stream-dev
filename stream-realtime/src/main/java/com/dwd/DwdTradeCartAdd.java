package com.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.util.CreateTopicDbToKafka;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd {


    private final static String REALTIME_KAFKA_DB_TOPIC = ConfigUtils.getString("kafka.topic.db");
    private final static String KAFKA_BOOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");
    private final static String TOPIC_DWD_TRADE_CART_ADD = ConfigUtils.getString("dwd.trade.cart.add");


    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettingUtils.defaultParameter(env);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

//        读取kafka数据

        CreateTopicDbToKafka.getKafkaTopicDb(tenv, REALTIME_KAFKA_DB_TOPIC,KAFKA_BOOTSTRAP_SERVERS, TOPIC_DWD_TRADE_CART_ADD);

//        tenv.executeSql("select * from topic_db").print();
//        过滤加购表
        Table cartAdd = filterCartAdd(tenv);

//        cartAdd.execute().print();
//建表
        getTableResult(tenv,TOPIC_DWD_TRADE_CART_ADD,KAFKA_BOOTSTRAP_SERVERS);


        cartAdd.insertInto(TOPIC_DWD_TRADE_CART_ADD).execute();



//        cartAdd.insertInto(KAFKA_GROUP);



    }


    public static void getTableResult(StreamTableEnvironment tenv,String dwd_trade_cart_add,String kafka_servers){

        tenv.executeSql("create table " + dwd_trade_cart_add + "(\n" +
                " id  STRING,\n" +
                " user_id STRING,\n" +
                " sku_id STRING,\n" +
                " cart_price STRING,\n" +
                " sku_num BIGINT,\n" +
                " sku_name STRING,\n" +
                " is_checked STRING,\n" +
                " create_time STRING,\n" +
                " operate_time STRING,\n" +
                " is_ordered STRING,\n" +
                " order_time STRING,\n" +
                "   ts BIGINT)" +
                "WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + dwd_trade_cart_add + "',\n" +
                "  'properties.bootstrap.servers' = '" + kafka_servers + "',\n" +
                "  'format' = 'json'\n" +
                ")");

    }

    public static Table filterCartAdd(StreamTableEnvironment tenv){
        return tenv.sqlQuery("select " +
                " `after`['id'] id, " +
                " `after`['user_id'] user_id, " +
                " `after`['sku_id'] sku_id, " +
                " `after`['cart_price'] cart_price, " +
                "  if(`op` in ('r','c'),cast(`after`['sku_num'] as bigint),cast(`after`['sku_num'] as bigint)-cast(`before`['sku_num'] as bigint)) sku_num, " +
                " `after`['sku_name'] sku_name, " +
                " `after`['is_checked'] is_checked, " +
                " `after`['create_time'] create_time, " +
                " `after`['operate_time'] operate_time, " +
                " `after`['is_ordered'] is_ordered, " +
                " `after`['order_time'] order_time," +
                "  ts_ms " +
                " from topic_db " +
                " where `source`['db']='gmall' " +
                " and `source`['table']='cart_info' " +
                " and (`op` in ('r','c') or (`op`='u' and `before`['sku_num'] is not null " +
                " and cast(`after`['sku_num'] as bigint) > cast(`before`['sku_num'] as bigint) ) ) ");
    }



}
