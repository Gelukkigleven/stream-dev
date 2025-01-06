package com.dwd;

import com.stream.common.utils.ConfigUtils;
import com.util.CreateTopicDbToKafka;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.dwd.DwdTtradeOrderCancelDetai
 * @Author hou.dz
 * @Date 2025/1/2 16:42
 * @description:
 */
public class DwdTtradeOrderCancelDetai {

    private final static String REALTIME_KAFKA_DB_TOPIC = ConfigUtils.getString("kafka.topic.db");
    private final static String KAFKA_BOOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");
    private final static String TOPIC_DWD_TRADE_ORDER_CANCEL_DETAIL = ConfigUtils.getString("dwd.trade.order.cancel.detail");

    private final static String TOPIC_DWD_TRADE_ORDER_DETAIL = ConfigUtils.getString("dwd.trade.order.detail");

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

//        创建表
        CreateTopicDbToKafka.getKafkaTopicDb(tenv, REALTIME_KAFKA_DB_TOPIC,KAFKA_BOOTSTRAP_SERVERS,TOPIC_DWD_TRADE_ORDER_CANCEL_DETAIL);

//        获取详情表
        getOrderDetail(tenv,TOPIC_DWD_TRADE_ORDER_CANCEL_DETAIL,TOPIC_DWD_TRADE_ORDER_DETAIL,KAFKA_BOOTSTRAP_SERVERS);

//        订单取消表
        getOrderCancle(tenv);


//        进行表关联
        Table tableJoin = getTableJoin(tenv);
//        tableJoin.execute().print();

        extracted1(tenv,TOPIC_DWD_TRADE_ORDER_CANCEL_DETAIL);
        tableJoin.insertInto(TOPIC_DWD_TRADE_ORDER_CANCEL_DETAIL).execute();


    }

    public static void extracted1(StreamTableEnvironment tenv,String tableName){

        tenv.executeSql("create table  "+tableName+"  (" +
                "  id  STRING,\n" +
                "  order_id  STRING,\n" +
                "  sku_id  STRING,\n" +
                "  user_id  STRING,\n" +
                "  province_id  STRING,\n" +
                "  activity_id  STRING,\n" +
                "  activity_rule_id  STRING,\n" +
                "  coupon_id  STRING,\n" +
                "  sku_name  STRING,\n" +
                "  order_price  STRING,\n" +
                "  sku_num  STRING,\n" +
                "  create_time  STRING,\n" +
                "  operate_time  STRING,\n" +
                "  split_total_amount  STRING,\n" +
                "  split_activity_amount  STRING,\n" +
                "  split_coupon_amount  STRING,\n" +
                "  ts bigint \n" +
                ") WITH ( " +
                "     'connector' = 'kafka'," +
                "     'topic' = ' "+tableName+" '," +
                "     'properties.bootstrap.servers' = '"+KAFKA_BOOTSTRAP_SERVERS+"', " +
                "     'format' = 'json'" +
                "   )" );
    }



    public static Table getTableJoin(StreamTableEnvironment tableEnv){

        return tableEnv.sqlQuery("select \n" +
                "  od.id,\n" +
                "  order_id,\n" +
                "  sku_id,\n" +
                "  user_id,\n" +
                "  province_id,\n" +
                "  activity_id,\n" +
                "  activity_rule_id,\n" +
                "  coupon_id,\n" +
                "  sku_name,\n" +
                "  order_price,\n" +
                "  sku_num,\n" +
                "  create_time,\n" +
                "  operate_time,\n" +
                "  split_total_amount,\n" +
                "  split_activity_amount,\n" +
                "  split_coupon_amount,\n" +
                "  oc.ts_ms \n" +
                "from dwd_trade_order_detail od\n" +
                "join order_cancel oc  on od.order_id = oc.id ");
    }


    private static void getOrderCancle(StreamTableEnvironment tableEnv) {
        Table table = tableEnv.sqlQuery(" select " +
                " `after`['id'] id, " +
                " `after`['operate_time'] operate_time, " +
                " ts_ms " +
                " from topic_db " +
                " where `source`['db']='gmall'\n" +
                " and `source`['table']='order_info'\n" +
                " and ( (`op` in ('c','r') and `after`['order_status'] = '1003' )  or " +
                " (`op`='u' and `before`['order_status'] = '1001' and `after`['order_status'] = '1003' ))" );
        tableEnv.createTemporaryView("order_cancel",table);
    }


    public static void getOrderDetail(StreamTableEnvironment tenv,String groupId,String topicName,String kafkaBBroker) {
        tenv.executeSql("create table dwd_trade_order_detail (\n" +
                "  id  STRING,\n" +
                "  order_id  STRING,\n" +
                "  sku_id  STRING,\n" +
                "  user_id  STRING,\n" +
                "  province_id  STRING,\n" +
                "  activity_id  STRING,\n" +
                "  activity_rule_id  STRING,\n" +
                "  coupon_id  STRING,\n" +
                "  sku_name  STRING,\n" +
                "  order_price  STRING,\n" +
                "  sku_num  STRING,\n" +
                "  create_time  STRING,\n" +
                "  split_total_amount  STRING,\n" +
                "  split_activity_amount  STRING,\n" +
                "  split_coupon_amount  STRING,\n" +
                "  ts bigint\n" +
                " ) WITH (" +
                "      'connector' = 'kafka', " +
                "      'topic' = '  "+topicName+"  '," +
                "      'properties.bootstrap.servers' = '  "+kafkaBBroker+"  ', " +
                "      'properties.group.id' = ' "+groupId+" '," +
                "      'scan.startup.mode' = 'earliest-offset'," +
                "      'format' = 'json'" +
                "    ) " );
    }
}

