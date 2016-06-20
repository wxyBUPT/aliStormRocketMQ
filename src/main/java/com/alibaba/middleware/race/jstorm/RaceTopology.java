package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.bolt.MiniuteTbTmTradeBolt;
import com.alibaba.middleware.race.jstorm.bolt.OrderWriteBolt;
import com.alibaba.middleware.race.jstorm.bolt.PayMessageDeserializeBolt;
import com.alibaba.middleware.race.jstorm.rocket.RocketSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by xiyuanbupt on 6/15/16.
 */
public class RaceTopology {

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
    private static Logger LOG = LoggerFactory.getLogger(Topology.class);

    private static final String TOPOLOGYNAME = RaceConfig.JstormTopologyName;

    public static void main(String[] args) throws Exception{

        TopologyBuilder builder = new TopologyBuilder();

        //使用环境变量获得rocketmq nameserver 的值
        String key = "rocketmq.namesrv.addr";
        String nameServer = System.getProperty(key);

        if (nameServer == null) {
            nameServer = "10.109.247.29:9876";
            //throw new Exception("并未指定rocketmq 的环境变量 rocketmq.namesrv.addr 的值");
        }

        RocketSpout tbOrderSpout = new RocketSpout(RaceConfig.MqTaoboaTradeTopic,
                RaceConfig.MetaConsumerGroup,
                nameServer);
        RocketSpout tmOrderSpout = new RocketSpout(RaceConfig.MqTmallTradeTopic,
                RaceConfig.MetaConsumerGroup,
                nameServer);

        RocketSpout paymentSpout = new RocketSpout(
                RaceConfig.MqPayTopic,
                RaceConfig.MetaConsumerGroup,
                nameServer,1
        );

        OrderWriteBolt tbOrderWriteBolt = new OrderWriteBolt();
        OrderWriteBolt tmOrderWriteBolt = new OrderWriteBolt();
        PayMessageDeserializeBolt payMessageDeserializeBolt =
                new PayMessageDeserializeBolt();
        //计算每分钟不同平台交易额比例的bolt
        MiniuteTbTmTradeBolt miniuteTbTmTradeBolt = new MiniuteTbTmTradeBolt();

        builder.setSpout(TBORDERMESSAGE_SPOUT_ID,tbOrderSpout,1);
        builder.setSpout(TMORDERMESSAGE_SPOUT_ID,tmOrderSpout,1);
        builder.setSpout(PAYMENTMESSAGE_SPOUT_ID,paymentSpout,1);

        builder.setBolt(TM_ORDERMESSAGE_WRITE_BOLT_ID,tmOrderWriteBolt,1)
                .shuffleGrouping(TMORDERMESSAGE_SPOUT_ID);
        builder.setBolt(TB_ORDERMESSAGE_WRITE_BOLT_ID,tbOrderWriteBolt,1)
                .shuffleGrouping(TBORDERMESSAGE_SPOUT_ID);
        builder.setBolt(PAYMENTMESSAGE_DESERIALIZE_BOLT_ID,payMessageDeserializeBolt
                ,1).shuffleGrouping(PAYMENTMESSAGE_SPOUT_ID);

        //builder.setBolt(CALCULATERATIO_BOLT_ID,calculateRatioBolt,1).shuffleGrouping(PAYMENTMESSAGE_DESERIALIZE_BOLT_ID);
        builder.setBolt(MINIUTETBTMTRADEBOLT_ID,miniuteTbTmTradeBolt,1).shuffleGrouping(PAYMENTMESSAGE_DESERIALIZE_BOLT_ID);

        Config config = new Config();

        try {
            StormSubmitter.submitTopology(TOPOLOGY_NAME,config,builder.createTopology());
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
