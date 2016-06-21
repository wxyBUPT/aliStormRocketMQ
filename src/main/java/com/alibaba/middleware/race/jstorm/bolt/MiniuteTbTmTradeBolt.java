package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.bolt.forUpdateTair.ReportTbTmTradeThread;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by xiyuanbupt on 6/15/16.
 */
public class MiniuteTbTmTradeBolt extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(MiniuteTbTmTradeBolt.class);

    private OutputCollector collector;
    private ConcurrentHashMap<Long,Double> tmMiniuteTrades = null;
    private ConcurrentHashMap<Long,Double> tbMiniuteTrades = null;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.tmMiniuteTrades = new ConcurrentHashMap<Long, Double>();
        this.tbMiniuteTrades = new ConcurrentHashMap<Long, Double>();

        //启动数据同步线程
        new Thread(new ReportTbTmTradeThread(RaceConfig.prex_tmall,this.tmMiniuteTrades)).start();
        new Thread(new ReportTbTmTradeThread(RaceConfig.prex_taobao,this.tbMiniuteTrades)).start();
    }

    @Override
    public void execute(Tuple tuple) {
        String plat_tm_tb = tuple.getStringByField("plat_tm_tb");
        long minuteTime = tuple.getLongByField("minuteTime");
        double payAmount = tuple.getDoubleByField("payAmount");
        StringBuilder sb = new StringBuilder();
        sb.append("得到平台信息和分钟信息");
        sb.append("平台信息是: ").append(plat_tm_tb);
        LOG.debug(sb.toString());
        if(plat_tm_tb.equals("tb") ){
            Double tbMiniuteTrade = tbMiniuteTrades.get(minuteTime);
            if(tbMiniuteTrade == null){
                tbMiniuteTrade = 0.0;
            }
            tbMiniuteTrade += payAmount;
            this.tbMiniuteTrades.put(minuteTime,tbMiniuteTrade);
            this.collector.emit(new Values(RaceConfig.prex_taobao,minuteTime,tbMiniuteTrade));
        }else if(plat_tm_tb.equals("tm") ){
            Double tmMiniuteTrade = tmMiniuteTrades.get(minuteTime);
            if(tmMiniuteTrade == null){
                tmMiniuteTrade = 0.0;
            }
            tmMiniuteTrade += payAmount;
            this.tmMiniuteTrades.put(minuteTime,tmMiniuteTrade);
            this.collector.emit(new Values(RaceConfig.prex_tmall,minuteTime,tmMiniuteTrade));
        }else {
            LOG.error("收到既不是淘宝平台也不是天猫平台的订单");
        }
        //为了调试写 error日志

        //try {
        //    OutputStream f = new FileOutputStream("./forDebug.txt");
        //    OutputStreamWriter writer = new OutputStreamWriter(f,"utf-8");
        //    StringBuilder sb = new StringBuilder();
        //    sb.append("得到平台信息和分钟信息");
        //    sb.append("plat_tm_tb:  ").append(plat_tm_tb);
        //    sb.append("minuteTime : ").append(minuteTime);
        //    writer.append("得到平台信息和分钟信息\r\n");
        //    writer.append(sb.toString());
        //    writer.close();
        //    f.close();
        //}catch (Exception e){
        //    e.printStackTrace();
        //}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(
                "tmOrTb",
                "time",
                "trade"
        ));
    }
}
