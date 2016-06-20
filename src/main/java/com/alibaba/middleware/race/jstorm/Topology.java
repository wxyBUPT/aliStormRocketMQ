package com.alibaba.middleware.race.jstorm;

import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.bolt.MiniutePcMbTradeBolt;
import com.alibaba.middleware.race.jstorm.bolt.MiniuteTbTmTradeBolt;
import com.alibaba.middleware.race.jstorm.bolt.OrderWriteBolt;
import com.alibaba.middleware.race.jstorm.bolt.PayMessageDeserializeBolt;
import com.alibaba.middleware.race.jstorm.rocket.RocketSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by xiyuanbupt on 6/8/16.
 */
public class Topology {

    private static String TBORDERMESSAGE_SPOUT_ID= "tbOrderMessageSpout";
    private static String TMORDERMESSAGE_SPOUT_ID = "tmOrderMessageSpout";
    private static String TM_ORDERMESSAGE_WRITE_BOLT_ID = "tmOrderMessageWriteBolt";
    private static String TB_ORDERMESSAGE_WRITE_BOLT_ID = "tbOrderMessageWriteBolt";
    private static String TOPOLOGY_NAME = RaceConfig.JstormTopologyName;
    private static String PAYMENTMESSAGE_SPOUT_ID = "paymentMessageSpout";
    private static String PAYMENTMESSAGE_DESERIALIZE_BOLT_ID
            = "paymentMessageDeserializeBolt";
    private static String CALCULATERATIO_BOLT_ID = "calculateRatioBolt";
    private static String MINIUTETBTMTRADEBOLT_ID = "minuteTbTmTradeBolt" ;
    private static String MINIUTEPCMBTRADEBOLT_ID = "minutePcMbTradeBolt";
    private static Logger LOG = LoggerFactory.getLogger(Topology.class);

    public static void main(String[] args) throws Exception{

        TopologyBuilder builder = setupBuilder();
        submitTopology(builder);
    }

    private static TopologyBuilder setupBuilder() throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        //使用环境变量获得rocketmq nameserver 的值
        String key = "rocketmq.namesrv.addr";
        String nameServer = System.getProperty(key);

        if (nameServer == null) {
            throw new Exception("并未指定rocketmq 的环境变量 rocketmq.namesrv.addr 的值");
        }

        //初始化三个bolt
        RocketSpout tbOrderSpout = new RocketSpout(
                RaceConfig.MqTaoboaTradeTopic,
                RaceConfig.MetaConsumerGroup + RaceConfig.MqTaoboaTradeTopic,
                nameServer);
        RocketSpout tmOrderSpout = new RocketSpout(
                RaceConfig.MqTmallTradeTopic,
                RaceConfig.MetaConsumerGroup + RaceConfig.MqTmallTradeTopic,
                nameServer);

        RocketSpout paymentSpout = new RocketSpout(
                RaceConfig.MqPayTopic,
                RaceConfig.MetaConsumerGroup+RaceConfig.MqPayTopic,
                nameServer,1
        );

        builder.setSpout(TBORDERMESSAGE_SPOUT_ID,tbOrderSpout,1);
        builder.setSpout(TMORDERMESSAGE_SPOUT_ID,tmOrderSpout,1);
        builder.setSpout(PAYMENTMESSAGE_SPOUT_ID,paymentSpout,1);

        //初始化两个订单信息同步到 Tair 中的bolt
        OrderWriteBolt tbOrderWriteBolt = new OrderWriteBolt();
        OrderWriteBolt tmOrderWriteBolt = new OrderWriteBolt();
        builder.setBolt(TM_ORDERMESSAGE_WRITE_BOLT_ID,tmOrderWriteBolt,1)
                .shuffleGrouping(TMORDERMESSAGE_SPOUT_ID);
        builder.setBolt(TB_ORDERMESSAGE_WRITE_BOLT_ID,tbOrderWriteBolt,1)
                .shuffleGrouping(TBORDERMESSAGE_SPOUT_ID);

        //解序列化付款信息,同时查看Tair 来自哪个交易平台
        PayMessageDeserializeBolt payMessageDeserializeBolt =
                new PayMessageDeserializeBolt();
        builder.setBolt(PAYMENTMESSAGE_DESERIALIZE_BOLT_ID,payMessageDeserializeBolt
                ,1).shuffleGrouping(PAYMENTMESSAGE_SPOUT_ID);


        //计算每分钟不同平台交易额比例的bolt
        MiniuteTbTmTradeBolt miniuteTbTmTradeBolt = new MiniuteTbTmTradeBolt();
        //builder.setBolt(CALCULATERATIO_BOLT_ID,calculateRatioBolt,1).shuffleGrouping(PAYMENTMESSAGE_DESERIALIZE_BOLT_ID);
        builder.setBolt(MINIUTETBTMTRADEBOLT_ID,miniuteTbTmTradeBolt,1).shuffleGrouping(PAYMENTMESSAGE_DESERIALIZE_BOLT_ID);
        //builder.setBolt(MINIUTETBTMTRADEBOLT_ID,miniuteTbTmTradeBolt,2).setNumTasks(2).fieldsGrouping(PAYMENTMESSAGE_DESERIALIZE_BOLT_ID,new Fields("plat_tm_tb"));

        //每分钟不同客户端交易额计算的bolt
        MiniutePcMbTradeBolt miniutePcMbTradeBolt = new MiniutePcMbTradeBolt();
        builder.setBolt(MINIUTEPCMBTRADEBOLT_ID,miniutePcMbTradeBolt,1).shuffleGrouping(PAYMENTMESSAGE_DESERIALIZE_BOLT_ID);

        return builder;
    }

    private static void submitTopology(TopologyBuilder builder){
        try{
            LocalCluster localCluster = new LocalCluster();
            Map conf = new HashMap();
            localCluster.submitTopology(TOPOLOGY_NAME,conf,builder.createTopology());
            Thread.sleep(2000000);
        }catch (Exception e){
            System.out.println("遇到了一些异常");
            System.out.println(e.getMessage()+ e.getClass());
        }
    }
}
