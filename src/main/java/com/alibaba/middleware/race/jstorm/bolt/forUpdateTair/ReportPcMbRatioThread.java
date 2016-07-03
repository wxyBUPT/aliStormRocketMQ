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
        sb.append("Thread : ").append(Thread.currentThread().getName());
        sb.append("  start initialize Tair client");
        LOG.info(sb.toString());
        tairOperator = TairOperatorImpl.getInstance();
        LOG.info("Create Tair client connection succeed!");
        this.pcMiniuteTrades = pcMiniuteTrades;
        this.mbMiniuteTrades = mbMiniuteTrades;
    }

    private void calculateRatioAndReport(){
        HashMap<Long,Double> pcMiniuteTotalTrades = miniuteTrades2miniuteTotalTradesmap(this.pcMiniuteTrades);
        HashMap<Long,Double> mbMiniuteTotalTrades = miniuteTrades2miniuteTotalTradesmap(this.mbMiniuteTrades);
        StringBuilder sb = new StringBuilder();
        sb.append("移动端和pc 端的交易比例为: ");
        long minMiniute,maxMiniute;
        minMiniute = getMapMinKey(pcMiniuteTotalTrades);
        maxMiniute = getMapMaxKey(pcMiniuteTotalTrades);
        for(long currentMiniute = minMiniute;currentMiniute<= maxMiniute;currentMiniute += 60){
            double pcTotalTrades,mbTotalTrades;
            try {
                pcTotalTrades = pcMiniuteTotalTrades.get(currentMiniute);
                mbTotalTrades = mbMiniuteTotalTrades.get(currentMiniute);
            }catch (Exception e){
                continue;
            }
            double ratio;
            try {
                ratio = mbTotalTrades / pcTotalTrades;
            }catch (Exception e){
                continue;
            }
            //为了不让Tair客户端负载压力过大,暂停执行
            try {
                Thread.sleep(5);
            }catch (Exception e){

            }
            try {
                sb.append(RaceConfig.prex_ratio + currentMiniute + " : " + ratio);
                sb.append("   ,  ");
                tairOperator.write(RaceConfig.prex_ratio + currentMiniute, ratio);
            }catch (Exception e){
                LOG.error("Key: " + RaceConfig.prex_ratio + currentMiniute+ " has not been writen to Tair , ratio is : " + ratio);
                LOG.trace(e.getStackTrace());
            }
        }
        LOG.info(sb.toString());
    }


    private HashMap<Long,Double> miniuteTrades2miniuteTotalTradesmap(ConcurrentHashMap<Long,Double> from){
        HashMap<Long,Double> miniuteTotalTradesmap = new HashMap<Long, Double>(from);
        long minMiniute = getMapMinKey(miniuteTotalTradesmap);
        long maxMiniute = getMapMaxKey(miniuteTotalTradesmap);

        double currentTotalTrades = from.get(minMiniute);

        for(long currentMiniute = minMiniute;currentMiniute<=maxMiniute;currentMiniute += 60){
            if(from.get(currentMiniute) != null){
                currentTotalTrades =+ currentTotalTrades + from.get(currentMiniute);
            }
            miniuteTotalTradesmap.put(currentMiniute,currentTotalTrades);
        }

        return miniuteTotalTradesmap;
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
                LOG.error("Some Exception happend while thread sleep");
            }
            if(pcMiniuteTrades.size() == 0 || mbMiniuteTrades.size() ==0){
                LOG.info("Wireless and Pc trades is empty, so will not perform data synchronization");
                continue;
            }
            calculateRatioAndReport();
        }
    }
}
