package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.bolt.CacheBolt;
import com.alibaba.middleware.race.jstorm.bolt.MiniutePcMbTradeBolt;
import com.alibaba.middleware.race.jstorm.bolt.MiniuteTbTmTradeBolt;
import com.alibaba.middleware.race.jstorm.rocket.RocketSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xiyuanbupt on 6/8/16.
 */
public class Topology {

    private static String ROCKETSPOUT_ID = "rocketSpoutId";
    private static String ORDERMESSAGE_WRITE_BOLT_ID = "orderMessageWriteBolt";

    private static String TOPOLOGY_NAME = RaceConfig.JstormTopologyName;
    private static String CACHEBOLTID = "cacheBolt";
    private static String CALCULATERATIO_BOLT_ID = "calculateRatioBolt";
    private static String MINIUTETBTMTRADEBOLT_ID = "minuteTbTmTradeBolt" ;
    private static String MINIUTEPCMBTRADEBOLT_ID = "minutePcMbTradeBolt";
    private static Logger LOG = LoggerFactory.getLogger(Topology.class);

    public static void main(String[] args) throws Exception{

        TopologyBuilder builder = setupBuilder();
        submitTopology(builder);
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
                1
        );

        builder.setSpout(ROCKETSPOUT_ID,rocketSpout,1).setNumTasks(1);


        System.out.println("我就是想不通");
        //解序列化付款信息,同时查看Tair 来自哪个交易平台
        CacheBolt cacheBolt=
                new CacheBolt();
        builder.setBolt(CACHEBOLTID,cacheBolt
                ,1).setNumTasks(4).shuffleGrouping(ROCKETSPOUT_ID);


        //计算每分钟不同平台交易额比例的bolt
        MiniuteTbTmTradeBolt miniuteTbTmTradeBolt = new MiniuteTbTmTradeBolt();
        builder.setBolt(MINIUTETBTMTRADEBOLT_ID,miniuteTbTmTradeBolt,1).shuffleGrouping(CACHEBOLTID);

        //每分钟不同客户端交易额计算的bolt
        MiniutePcMbTradeBolt miniutePcMbTradeBolt = new MiniutePcMbTradeBolt();
        builder.setBolt(MINIUTEPCMBTRADEBOLT_ID,miniutePcMbTradeBolt,1).shuffleGrouping(CACHEBOLTID);

        return builder;
    }

    private static void submitTopology(TopologyBuilder builder){
        try{
            LocalCluster localCluster = new LocalCluster();
            Map conf = new HashMap();
            Config.setNumAckers(conf,0);
            localCluster.submitTopology(TOPOLOGY_NAME,conf,builder.createTopology());
            Thread.sleep(2000000);
        }catch (Exception e){
            System.out.println("遇到了一些异常");
            System.out.println(e.getMessage()+ e.getClass());
        }
    }
}
