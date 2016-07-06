package com.alibaba.middleware.race.jstorm.bolt.forUpdateTair;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.tair.TairOperatorImpl;
import com.taobao.tair.DataEntry;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by xiyuanbupt on 6/19/16.
 * 将将本地内容 HashMap 中的数据同步到 Tair 中,定期执行
 */
public class ReportTbTmTradeThread implements Runnable {

    private static final Logger LOG = Logger.getLogger(ReportTbTmTradeThread.class);

    protected ConcurrentHashMap<Long,Double> hashMap;
    private String prefix;
    private TairOperatorImpl tairOperator;


    public ReportTbTmTradeThread(String prefix, ConcurrentHashMap<Long,Double> hashMap) {
        initTairConnection();
        this.hashMap = hashMap;
        this.prefix = prefix;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(RaceConfig.ReportTbTmTradeInterval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            report();
        }
    }

    private void initTairConnection(){
        StringBuilder sb = new StringBuilder();
        sb.append("Thread: ").append(Thread.currentThread().getName());
        sb.append("initialize Tari client : ");
        LOG.info(sb.toString());
        tairOperator = TairOperatorImpl.getInstance();
    }

    private boolean report(){
        StringBuilder sb = new StringBuilder();
        sb.append("TmTbTradeSynchronization: ");
        sb.append(" current plat form : ").append(prefix);
        sb.append("current plat trades per miniute").append(hashMap);
        LOG.info(sb.toString());
        for(Map.Entry<Long,Double> entry : hashMap.entrySet()){
            String key = this.prefix + entry.getKey();
            Double value = (double)Math.round(entry.getValue()*100)/100;
            try {
               Thread.sleep(5);
            }catch (Exception e){
            }
            try {
                tairOperator.write(key,value);
            }catch (Exception e){
                LOG.error("Key: " + key + " has not been writen to Tair , value is : " + value );
                LOG.trace(e.getStackTrace());
            }
        }
        return true;
    }
}
