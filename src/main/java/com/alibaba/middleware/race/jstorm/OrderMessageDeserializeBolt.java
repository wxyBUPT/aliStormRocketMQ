package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by xiyuanbupt on 6/7/16.
 * 接收TbOrderMessage Spout TmOrderMessage Spout 的tuple ,并解序列化
 *
 */
public class OrderMessageDeserializeBolt implements IRichBolt{
    private static final Logger LOG = LoggerFactory.getLogger(OrderMessageDeserializeBolt.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map config, TopologyContext context,OutputCollector collector){

        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple){
        System.out.println("执行了 execute 方法");
        Object msgObj = tuple.getValue(0);
        try{
            System.out.println("下面是从rocketmq 中获得的消息");
            System.out.println("Message:" + msgObj + "\n statistics:"  );
        }catch (Exception e){
            collector.fail(tuple);
            return;
        }
        collector.ack(tuple);
    }

    @Override
    public void cleanup(){
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){

    }

    @Override
    public Map<String,Object> getComponentConfiguration(){
        return null;
    }
}
