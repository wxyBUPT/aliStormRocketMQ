package com.alibaba.middleware.race.helloStrom;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created by xiyuanbupt on 5/30/16.
 */
public class HelloWorldTopology  {

    public static void main(String[] args)throws Exception{
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Hello",new HelloWorldSpout(),12);
        builder.setBolt("world",new HelloWorldBolt(),12).shuffleGrouping("Hello");
        builder.setBolt("WorldTwo",new HelloWorldBolt(),12).shuffleGrouping("World");

        Config config = new Config();
        config.setDebug(true);

        if(args != null && args.length>0){
            config.setNumWorkers(6);
            config.setNumAckers(6);
            config.setMaxSpoutPending(100);
            config.setMessageTimeoutSecs(20);
            StormSubmitter.submitTopology(args[0],config,builder.createTopology());
        }else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Hello-World-Storm",config,builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("Hello-World-Storm");
            cluster.shutdown();
        }
    }
}
