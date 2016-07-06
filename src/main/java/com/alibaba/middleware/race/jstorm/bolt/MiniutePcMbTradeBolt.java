package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.jstorm.bolt.forUpdateTair.ReportPcMbRatioThread;
import com.alibaba.middleware.race.model.PaymentMessage;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by xiyuanbupt on 6/19/16.
 */
public class MiniutePcMbTradeBolt extends BaseRichBolt{

    private static final Logger LOG = Logger.getLogger(MiniutePcMbTradeBolt.class);

    private OutputCollector collector ;
    private ConcurrentHashMap<Long,Double> pcMiniuteTrades = null;
    private ConcurrentHashMap<Long,Double> mbMiniuteTrades = null;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.pcMiniuteTrades = new ConcurrentHashMap<Long, Double>();
        this.mbMiniuteTrades = new ConcurrentHashMap<Long, Double>();

        //启动数据同步线程
        new Thread(new ReportPcMbRatioThread(pcMiniuteTrades,mbMiniuteTrades)).start();
    }

    @Override
    public void execute(Tuple tuple) {
        String type = tuple.getStringByField("type");
        if(type.equals("order"))return;
        PaymentMessage paymentMessage = (PaymentMessage)tuple.getValueByField("class");
        Short plat_pc_mb = paymentMessage.getPayPlatform();
        Long createTime = paymentMessage.getCreateTime();
        Long minuteTime = (createTime/1000/60) * 60;
        double payAmount = paymentMessage.getPayAmount();
        //0 代表pc 交易
        if(plat_pc_mb == 0){
            //对pc 端的交易额进行计算
            Double pcMiniuteTrade = pcMiniuteTrades.get(minuteTime);
            if(pcMiniuteTrade == null){
                pcMiniuteTrade = 0.0;
            }
            pcMiniuteTrade += payAmount;
            this.pcMiniuteTrades.put(minuteTime,pcMiniuteTrade);
            this.collector.emit(new Values(0,minuteTime,pcMiniuteTrade));
        }else if(plat_pc_mb == 1){
            //对 mb 端的交易额进行计算
            Double mbMiniuteTrade = mbMiniuteTrades.get(minuteTime);
            if(mbMiniuteTrade == null){
                mbMiniuteTrade = 0.0;
            }
            mbMiniuteTrade += payAmount;
            this.mbMiniuteTrades.put(minuteTime,mbMiniuteTrade);
            this.collector.emit(new Values(1,minuteTime,mbMiniuteTrade));
        } else  {
            LOG.error("Trade plat form is neither pc nor wireless" + plat_pc_mb);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(
                "pcOrMb",
                "time",
                "trade"
        ));
    }
}
