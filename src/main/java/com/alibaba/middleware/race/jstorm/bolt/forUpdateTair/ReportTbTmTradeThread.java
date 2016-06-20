package com.alibaba.middleware.race.jstorm.bolt.forUpdateTair;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.tair.TairOperatorImpl;
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
        sb.append("线程").append(Thread.currentThread().getName());
        sb.append("初始化Tair 客户端");
        LOG.info(sb.toString());
        tairOperator = new TairOperatorImpl(
                RaceConfig.TairConfigServer,
                RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup,
                RaceConfig.TairNamespace
        );
    }

    private boolean report(){
        StringBuilder sb = new StringBuilder();
        sb.append("执行一次数据同步工作,同步的平台是").append(prefix);
        sb.append("当前该平台每分钟交易额为").append(hashMap.toString());
        LOG.info(sb.toString());
        for(Map.Entry<Long,Double> entry : hashMap.entrySet()){
            String key = this.prefix + entry.getKey();
            Double value = (double)Math.round(entry.getValue()*100)/100;
            tairOperator.write(key,value);
        }
        return true;
    }
}
