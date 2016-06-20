package com.alibaba.middleware.race.jstorm.rocket.example;

import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.rocket.RocketSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by xiyuanbupt on 6/8/16.
 */
public class TestTopology {

    private static String TBSPOUT_ID = "tb_spout";
    private static String TBBOLT_ID = "tb_bolt";
    private static String TOPOLOGY_NAME = "fortest";

    private static Logger LOG = LoggerFactory.getLogger(TestTopology.class);

    public static void main(String[] args)throws Exception{
        TopologyBuilder builder = setupBuilder();
        submitTopology(builder);
    }

    private static TopologyBuilder setupBuilder() throws Exception{
        TopologyBuilder builder = new TopologyBuilder();

        //使用环境变量获得RocketMq NameServer 的值
        String key = "rocketmq.namesrv.addr";
        String nameServer = System.getProperty(key);

        if(nameServer == null){
            throw new Exception("未指定环境变量 rocketmq.namesrv.addr的值");
        }

        RocketSpout tbOrderSpout = new RocketSpout(RaceConfig.MqTaoboaTradeTopic,
                RaceConfig.MetaConsumerGroup,nameServer);
        RocketBoltForLearn rocketBoltForLearn = new RocketBoltForLearn();

        builder.setSpout(TBSPOUT_ID,tbOrderSpout,1);
        builder.setBolt(TBBOLT_ID,rocketBoltForLearn,1).shuffleGrouping(TBSPOUT_ID);

        return builder;
    }

    private static void submitTopology(TopologyBuilder builder){
        try {
            System.out.println("就是为了添加一行不同的内容");
            LocalCluster cluster = new LocalCluster();
            Map conf = new HashMap();
            cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());

            Thread.sleep(200000);
        }catch (Exception e){
            System.out.println(e.getMessage()+e.getClass());
        }

    }
}
