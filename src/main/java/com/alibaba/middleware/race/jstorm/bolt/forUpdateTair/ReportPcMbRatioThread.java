package com.alibaba.middleware.race.jstorm.bolt.forUpdateTair;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.tair.TairOperatorImpl;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by xiyuanbupt on 6/20/16.
 */
public class ReportPcMbRatioThread implements Runnable{

    private static Logger LOG = Logger.getLogger(ReportPcMbRatioThread.class);

    ConcurrentHashMap<Long,Double> pcMiniuteTrades;
    ConcurrentHashMap<Long,Double> mbMiniuteTrades;
    ConcurrentHashMap<Long,Double> mbPcRatio;

    private TairOperatorImpl tairOperator = null;

    public ReportPcMbRatioThread(ConcurrentHashMap<Long,Double> pcMiniuteTrades,ConcurrentHashMap<Long,Double> mbMiniuteTrades){
        StringBuilder sb = new StringBuilder();
        sb.append("线程: ").append(Thread.currentThread().getName());
        sb.append("  初始化 Tair 客户端");
        LOG.info(sb.toString());
        tairOperator = new TairOperatorImpl(
                RaceConfig.TairConfigServer,
                RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup,
                RaceConfig.TairNamespace
        );
        LOG.info("创建Tair 链接成功");
        this.pcMiniuteTrades = pcMiniuteTrades;
        this.mbMiniuteTrades = mbMiniuteTrades;
    }

    private void calculateRatioAndReport(){
        HashMap<Long,Double> pcMiniuteTotalTrades = miniuteTrades2miniuteTotalTradesmap(this.pcMiniuteTrades);
        HashMap<Long,Double> mbMiniuteTotalTrades = miniuteTrades2miniuteTotalTradesmap(this.mbMiniuteTrades);
        StringBuilder sb = new StringBuilder();
        sb.append("无线端分钟数的总交易额为 " ).append(mbMiniuteTotalTrades);
        sb.append("pc 端分钟数的总交易额为").append(pcMiniuteTotalTrades);
        LOG.info(sb.toString());
        long minMiniute,maxMiniute;
        minMiniute = getMapMinKey(pcMiniuteTotalTrades);
        maxMiniute = getMapMaxKey(pcMiniuteTotalTrades);
        for(long currentMiniute = minMiniute;currentMiniute<= maxMiniute;currentMiniute ++){
            double pcTotalTrades,mbTotalTrades;
            try {
                pcTotalTrades = pcMiniuteTotalTrades.get(currentMiniute);
                mbTotalTrades = mbMiniuteTotalTrades.get(currentMiniute);
            }catch (Exception e){
                LOG.error("无线端和 pc端 数据不同步" + "无线端的数据是: "+ mbMiniuteTotalTrades.toString() + "pc 端的数据是: " + pcMiniuteTotalTrades.toString());
                continue;
            }
            double ratio;
            try {
                ratio = mbTotalTrades / pcTotalTrades;
            }catch (Exception e){
                continue;
            }
            tairOperator.write(RaceConfig.prex_ratio + currentMiniute,ratio);
        }
    }

    private List<Map.Entry<Long,Double>> miniuteTrades2miniuteTotalTrades(ConcurrentHashMap<Long,Double> from){
        List<Map.Entry<Long,Double>> miniuteTotalTrades = new ArrayList<Map.Entry<Long, Double>>(from.entrySet());

        Collections.sort(miniuteTotalTrades, new Comparator<Map.Entry<Long, Double>>() {
            @Override
            public int compare(Map.Entry<Long, Double> o1, Map.Entry<Long, Double> o2) {
                return (o1.getKey().compareTo(o2.getKey()));
            }
        });

        for(int i = 1;i<miniuteTotalTrades.size();i++){
            Map.Entry<Long,Double> preEntry = miniuteTotalTrades.get(i-1);
            Map.Entry<Long,Double> curEntry = miniuteTotalTrades.get(i);
            curEntry.setValue(preEntry.getValue() + curEntry.getValue());
        }

        return miniuteTotalTrades;
    }

    private HashMap<Long,Double> miniuteTrades2miniuteTotalTradesmap(ConcurrentHashMap<Long,Double> from){
        HashMap<Long,Double> miniuteTotalTradesmap = new HashMap<Long, Double>(from);
        long minMiniute = getMapMinKey(miniuteTotalTradesmap);
        long maxMiniute = getMapMaxKey(miniuteTotalTradesmap);

        double currentTotalTrades = from.get(minMiniute);

        for(long currentMiniute = minMiniute;currentMiniute<=maxMiniute;currentMiniute++){
            if(from.get(currentMiniute) != null){
                currentTotalTrades =+ currentTotalTrades + from.get(currentMiniute);
            }
            miniuteTotalTradesmap.put(currentMiniute,currentTotalTrades);
        }

        return miniuteTotalTradesmap;
    }

    public HashMap<Long,Double> forTest(){
        return miniuteTrades2miniuteTotalTradesmap(this.mbMiniuteTrades);
    }

    private long getMapMinKey(HashMap<Long,Double> hashMap){
        long minVal = 9999999999L;
        for(long key : hashMap.keySet()){
            minVal = minVal<key? minVal:key;
        }
        return minVal;
    }

    private long getMapMaxKey(HashMap<Long,Double> hashMap){
        long maxVal = 0L;
        for(long key: hashMap.keySet()){
            maxVal = maxVal > key ? maxVal:key;
        }
        return maxVal;
    }

    @Override
    public void run() {
        while (true){
            try {
                Thread.sleep(RaceConfig.ReportRatioToTairInterval);
            }catch (Exception e){
                LOG.error("Thread sleep 发生了一些异常");
            }
            if(pcMiniuteTrades.size() == 0 || mbMiniuteTrades.size() ==0){
                LOG.info("当前的pc 和 mb 的交易额是空的,不执行数据同步操作");
                continue;
            }
            StringBuilder sb = new StringBuilder();
            sb.append("执行一次pc 端和无线端的总交易额比例同步,当前pc 每分钟交易额为: " + pcMiniuteTrades.toString() + "当前无线端每分钟交易额为 " + mbMiniuteTrades.toString());
            LOG.info(sb.toString());
            calculateRatioAndReport();
        }
    }
}