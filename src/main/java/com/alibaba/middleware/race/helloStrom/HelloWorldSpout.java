package com.alibaba.middleware.race.helloStrom;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 * Created by xiyuanbupt on 5/30/16.
 */
public class HelloWorldSpout extends BaseRichSpout {
    SpoutOutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer){
        declarer.declare(new Fields("hello"));
    }

    @Override
    public void open(Map map, TopologyContext context,
                     SpoutOutputCollector spoutOutputCollector){
        collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple(){
        Utils.sleep(1000);
        collector.emit(new Values("Hello"));
    }
}
