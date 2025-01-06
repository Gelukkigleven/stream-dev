package com.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.util.CreateTopicDbToKafka;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.dwd.DwdTradeOrderDetail
 * @Author hou.dz
 * @Date 2025/1/2 16:00
 * @description:
 */
public class DwdTradeOrderDetail {

    private final static String REALTIME_KAFKA_DB_TOPIC = ConfigUtils.getString("kafka.topic.db");
    private final static String KAFKA_BOOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");
    private final static String TOPIC_DWD_TRADE_ORDER_DETAIL = ConfigUtils.getString("dwd.trade.order.detail");

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.setParallelism(1);


        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

//     读取kafka中的数据
        CreateTopicDbToKafka.getKafkaTopicDb(tenv, REALTIME_KAFKA_DB_TOPIC,KAFKA_BOOTSTRAP_SERVERS,TOPIC_DWD_TRADE_ORDER_DETAIL);

//     过滤出订单详情表
        filterOrderDetailInfo(tenv);


//     过滤出订单表
        filterOrderInfo(tenv);
//      活动表
        filterOrderActivity(tenv);
//      优惠表
        filterOrderCoupon(tenv);

//      关联表
        Table table = OrderJoin(tenv);


        getSinkDataToKafka(tenv,TOPIC_DWD_TRADE_ORDER_DETAIL);

        table.insertInto(TOPIC_DWD_TRADE_ORDER_DETAIL).execute();



    }

    public static void getSinkDataToKafka(StreamTableEnvironment tenv,String topicName){

        tenv.executeSql("create table " + topicName + " (\n" +
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
                "  ts_ms bigint,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED \n" +
                ")   WITH (  " +
                "  'connector' = 'upsert-kafka'," +
                "  'topic' = '"+topicName+"', " +
                "  'properties.bootstrap.servers' = '   "+ KAFKA_BOOTSTRAP_SERVERS +  " ', " +
                "  'key.format' = 'json', " +
                "  'value.format' = 'json' " +
                "  )");

    }


    //    关联表
    public static Table OrderJoin(StreamTableEnvironment tenv){

        return tenv.sqlQuery("select \n" +
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
                "  split_total_amount,\n" +
                "  split_activity_amount,\n" +
                "  split_coupon_amount,\n" +
                "  ts_ms \n" +
                "from order_detail_info od\n" +
                "join order_info oi\n" +
                "on od.order_id = oi.id\n" +
                "left join order_detail_activity oda\n" +
                "on oda.id = od.id\n" +
                "left join order_detail_coupon odc\n" +
                "on odc.id = od.id ");

    }
    //    优惠表
    public static void filterOrderCoupon(StreamTableEnvironment tenv){
        Table odcTable = tenv.sqlQuery("select \n" +
                "  `after`['order_detail_id'] id, \n" +
                "  `after`['coupon_id'] coupon_id\n" +
                "from topic_db\n" +
                "where `source`['db']='gmall'\n" +
                "and `source`['table']='order_detail_coupon'\n" +
                "and `op` in ('r','c')");
        tenv.createTemporaryView("order_detail_coupon", odcTable);
    }

    //     活动表
    public static void filterOrderActivity(StreamTableEnvironment tenv){
        Table odaTable = tenv.sqlQuery("select \n" +
                "  `after`['order_detail_id'] id, \n" +
                "  `after`['activity_id'] activity_id, \n" +
                "  `after`['activity_rule_id'] activity_rule_id\n" +
                "from topic_db\n" +
                "where `source`['db']='gmall'\n" +
                "and `source`['table']='order_detail_activity'\n" +
                "and `op` in ('r','c')");
        tenv.createTemporaryView("order_detail_activity", odaTable);


    }


    //    过滤出订单表
    public static void filterOrderInfo(StreamTableEnvironment tenv){
        Table oiTable = tenv.sqlQuery("select \n" +
                "  `after`['id'] id, \n" +
                "  `after`['user_id'] user_id, \n" +
                "  `after`['province_id'] province_id \n" +
                "from topic_db " +
                "where `source`['db']='gmall' " +
                "and `source`['table']='order_info' " +
                "and `op` in ('r','c')");
        tenv.createTemporaryView("order_info", oiTable);
    }

//    过滤出订单详情表

    public static void filterOrderDetailInfo(StreamTableEnvironment tenv){
        Table odTable = tenv.sqlQuery("select \n" +
                "  `after`['id'] id, \n" +
                "  `after`['order_id'] order_id, \n" +
                "  `after`['sku_id'] sku_id, \n" +
                "  `after`['sku_name'] sku_name, \n" +
                "  `after`['order_price'] order_price, \n" +
                "  `after`['sku_num'] sku_num, \n" +
                "  `after`['create_time'] create_time, \n" +
                "  `after`['split_total_amount'] split_total_amount, \n" +
                "  `after`['split_activity_amount'] split_activity_amount, \n" +
                "  `after`['split_coupon_amount'] split_coupon_amount, \n" +
                "  ts_ms " +
                "from topic_db\n" +
                "where `source`['db'] = 'gmall'\n" +
                "and `source`['table']='order_detail'\n" +
                "and `op` in ('r','c')");
        tenv.createTemporaryView("order_detail_info", odTable);
    }

}

