package com.alibaba.middleware.race.jstorm.rocket.example;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.jstorm.rocket.RocketTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by xiyuanbupt on 6/8/16.
 */
public class RocketBoltForLearn implements IRichBolt{
    private static final Logger LOG = LoggerFactory.getLogger(RocketBoltForLearn.class);
    protected OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context,OutputCollector collector){
        this.collector = collector;
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void execute(Tuple tuple) {

        RocketTuple rocketTuple = (RocketTuple)tuple.getValue(0);

        try{
            System.out.println("成功的从Rocket 获得了一个tuple,接下来打印这个tuple");
            System.out.println(rocketTuple);
        }catch (Exception e){
            collector.fail(tuple);
            return;
        }
        collector.ack(tuple);
    }
}
