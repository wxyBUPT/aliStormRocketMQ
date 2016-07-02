package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.bolt.MiniutePcMbTradeBolt;
import com.alibaba.middleware.race.jstorm.bolt.MiniuteTbTmTradeBolt;
import com.alibaba.middleware.race.jstorm.bolt.PayMessageDeserializeBolt;
import com.alibaba.middleware.race.jstorm.rocket.RocketSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xiyuanbupt on 6/15/16.
 */
public class RaceTopology {

    private static String ROCKETSPOUT_ID = "rocketSpoutId";
    private static String ORDERMESSAGE_WRITE_BOLT_ID = "orderMessageWriteBolt";
    private static String TOPOLOGY_NAME = RaceConfig.JstormTopologyName;

    private static String PAYMENTMESSAGE_DESERIALIZE_BOLT_ID
            = "paymentMessageDeserializeBolt";
    private static String MINIUTETBTMTRADEBOLT_ID = "miniuteTbTmTradeBolt" ;
    private static String MINIUTEPCMBTRADEBOLT_ID = "miniutePcMbTradeBolt";
    private static Logger LOG = LoggerFactory.getLogger(Topology.class);

    public static void main(String[] args) throws Exception{

        TopologyBuilder builder = setupBuilder();

        //取消ack
        Map conf = new HashMap();
        Config.setNumAckers(conf,0);
        Config.setNumWorkers(conf,2);

        try {
            StormSubmitter.submitTopology(TOPOLOGY_NAME,conf,builder.createTopology());
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static TopologyBuilder setupBuilder() throws Exception{

        TopologyBuilder builder = new TopologyBuilder();

        //初始化三个bolt
        List<String> allTopic = new ArrayList<String>();
        allTopic.add(RaceConfig.MqPayTopic);
        allTopic.add(RaceConfig.MqTaoboaTradeTopic);
        allTopic.add(RaceConfig.MqTmallTradeTopic);
        RocketSpout rocketSpout = new RocketSpout(
                allTopic,
                RaceConfig.MetaConsumerGroup,
                6
        );
        builder.setSpout(ROCKETSPOUT_ID,rocketSpout,2).setNumTasks(2);


        //解序列化付款信息,同时查看Tair 来自哪个交易平台
        PayMessageDeserializeBolt payMessageDeserializeBolt =
                new PayMessageDeserializeBolt();
        builder.setBolt(PAYMENTMESSAGE_DESERIALIZE_BOLT_ID,payMessageDeserializeBolt
                ,4).setNumTasks(2).shuffleGrouping(ROCKETSPOUT_ID);


        //计算每分钟不同平台交易额比例的bolt
        MiniuteTbTmTradeBolt miniuteTbTmTradeBolt = new MiniuteTbTmTradeBolt();
        builder.setBolt(MINIUTETBTMTRADEBOLT_ID,miniuteTbTmTradeBolt,1).shuffleGrouping(PAYMENTMESSAGE_DESERIALIZE_BOLT_ID);

        //每分钟不同客户端交易额计算的bolt
        MiniutePcMbTradeBolt miniutePcMbTradeBolt = new MiniutePcMbTradeBolt();
        builder.setBolt(MINIUTEPCMBTRADEBOLT_ID,miniutePcMbTradeBolt,1).shuffleGrouping(PAYMENTMESSAGE_DESERIALIZE_BOLT_ID);

        return builder;
    }
}
