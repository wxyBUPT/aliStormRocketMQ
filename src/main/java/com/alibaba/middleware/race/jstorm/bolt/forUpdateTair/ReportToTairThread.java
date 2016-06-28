package com.alibaba.middleware.race.jstorm.bolt.forUpdateTair;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.tair.TairOperatorImpl;
import com.taobao.tair.DataEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by xiyuanbupt on 6/10/16.
 */
//编写将结果存储到Tair 的线程
//线程逻辑是每隔一段时间读取 ConcurrentHashMap 的数据,然后将结果存储到 Tair 中,并青空Concurrent
//里面的数据,并根据Concurrent 里面的 key 值更新Tair 时间戳的值
public class ReportToTairThread implements Runnable{


    private static final Logger LOG = LoggerFactory.getLogger(ReportToTairThread.class);

    protected ConcurrentHashMap<Long,Double> pcMiniuteTrades;
    protected ConcurrentHashMap<Long,Double> mbMiniuteTrades;
    private TairOperatorImpl tairOperator = null;
    private static final String PC_MINIUTE_TRADE_PREFIX = "pc_miniute_trade_prefix";
    private static final String MB_MINIUTE_TRADE_PREFIX = "mb_miniute_trade_prefix";
    private static final String MB_PC_RATIO_PREFIX = RaceConfig.prex_ratio;
    //维护一个当前最大的时间戳,每一次对Tair 的更新操作,都需要更新到当前的时间戳
    private long currentMaxMiniuteTime;


    public void run(){
        LOG.info("开始数据同步线程");
        //将 ConcurrentHashMap 的结果相加,并存储到一个新的HashMap 中
        while (true) {
            try {
                //每隔半分钟执行一次操作
                Thread.sleep(RaceConfig.ReportRatioToTairInterval);
            } catch (InterruptedException e) {
                continue;
            }
            updateAll();
        }
    }

    private void initTairConnection(){
        StringBuilder sb = new StringBuilder();
        sb.append("线程").append(Thread.currentThread().getName());
        sb.append("初始化Tair 客户端");
        LOG.info(sb.toString());
        tairOperator = TairOperatorImpl.getInstance();
        return ;
    }

    public ReportToTairThread(ConcurrentHashMap<Long,Double> pcMiniuteTrades,ConcurrentHashMap<Long,Double> mbMiniuteTrades){
        initTairConnection();
        this.pcMiniuteTrades = pcMiniuteTrades;
        this.mbMiniuteTrades = mbMiniuteTrades;
    }

    //map 的key 为一个时间戳信息,本函数将一个map 中的元素
    //以时间戳排序,并更新时间戳的value 信息,使得value 信息为
    //此前所有value 值的和
    private List<Map.Entry<Long,Double>> getLadderSumFromMap(Map map){
        List<Map.Entry<Long,Double>> forSoutSumList = new ArrayList<Map.Entry<Long, Double>>(
                map.entrySet()
        );
        //排序
        Collections.sort(forSoutSumList, new Comparator<Map.Entry<Long, Double>>() {
            @Override
            public int compare(Map.Entry<Long, Double> o1, Map.Entry<Long, Double> o2) {
                return (o1.getKey().compareTo(o2.getKey()));
            }
        });
        //将结果相加
        for(int i = 1;i<forSoutSumList.size();i++){
            Map.Entry<Long,Double> preEntry = forSoutSumList.get(i-1);
            Map.Entry<Long,Double> curEntry = forSoutSumList.get(i);
            curEntry.setValue(preEntry.getValue() + curEntry.getValue());
        }
        //更新当前最大的时间戳
        currentMaxMiniuteTime = forSoutSumList.get(forSoutSumList.size() - 1).getKey();
        return forSoutSumList;
    }

    //求两个concurrenthashMap 的并集
    //因为后来发现算法不对,所以下面的函数也不会再被使用
    private Set<Long> getKeySet(){
        Set<Long> s = new HashSet<Long>(mbMiniuteTrades.keySet());
        s.addAll(pcMiniuteTrades.keySet());
        return s;
    }

    private void tairAdder(String prefix,long from,long to , double value){
        //Tair 中key 属于 [prefix_from:prefix_to 不包括 prefix_to)
        // 中的值会增加 value 值,如果 key 值为空
        //那么初始化为 value
        StringBuilder sb = new StringBuilder();
        sb.append("针对 : ").append(prefix).append("增加每分钟的总交易额   ");
        sb.append("执行tairAdder(更新一段时间的Tair 数据),开始时间戳是: ").append(from);
        sb.append("结束时间是: ").append(to);
        sb.append("    ,每分钟交易额的增量为: ").append(value);
        LOG.info(sb.toString());
        String key;
        for(long i = from;i<to;i++){
            try {
                double newValue;
                key = prefix + i;
                DataEntry entry = tairOperator.get(key);
                //处理没有值的情况
                if (entry == null) {
                    newValue = value;
                } else {
                    newValue = new Double(entry.getValue().toString()) + value;
                }
                tairOperator.write(key,newValue);
                //尝试能不能从Tair 中读取相关的数据
                entry = tairOperator.get(key);
                sb.setLength(0);
                sb.append("成功将key 写入Tair 中").append("  , 写入的key 值是 key: ").append(entry.getKey());
                sb.append("写入的value 的值为:  ").append(entry.getValue());
                //LOG.info(sb.toString());
            }catch (Exception e){
                LOG.error(e.toString());
            }
        }
        return;
    }

    //提供给如下函数的list 需要保证key 是有序的,算法需要注意的一点是
    private void updateTrade(List<Map.Entry<Long,Double>> sortedLadderList,String prefix){
        //下面将会执行一大堆的 Tair 操作
        for(int i = 1;i<sortedLadderList.size();i++){
            long from = sortedLadderList.get(i-1).getKey();
            long to = sortedLadderList.get(i).getKey();
            double value = sortedLadderList.get(i-1).getValue();
            tairAdder(prefix,from,to,value);
        }
        return;
    }

    //更新Tair 端的 pc 前一段时间的交易额
    private void updatePcTrade(List<Map.Entry<Long,Double>> sortedLadderList){
        updateTrade(sortedLadderList,PC_MINIUTE_TRADE_PREFIX);
        LOG.debug("更新Pc端每分钟的交易额成功");
    }

    //更新Tair 端的 mb 前一段时间的交易额
    private void updateMbTrade(List<Map.Entry<Long,Double>> sortedLadderList){
        updateTrade(sortedLadderList,MB_MINIUTE_TRADE_PREFIX);
        LOG.debug("更新Mb端每分钟的总交易额成功");
    }

    //更新Tair 端的 pc mb 交易比例
    private void updateMbPcRatio(long fromTime,long toTime){
        StringBuilder sb = new StringBuilder();
        sb.append("执行 updateMbPcRatio,开始时间是").append(fromTime).append("结束时间是: ").append(toTime);
        LOG.info(sb.toString());
        //更新 mbPcRatio 在时间段 [fromTime,toTime] 的值
        //逻辑是读 Tair 中的交易额,然后更新
        String pcKey,mbKey,ratioKey;
        double pcValue,mbValue;
        for(long i = fromTime;i<=toTime;i++){
            pcKey = PC_MINIUTE_TRADE_PREFIX + i;
            mbKey = MB_MINIUTE_TRADE_PREFIX + i;
            ratioKey = MB_PC_RATIO_PREFIX + i;
            //读Tair 并更新相关的值
            DataEntry mbEntry = tairOperator.get(mbKey);
            DataEntry pcEntry = tairOperator.get(pcKey);
            if(mbEntry == null || pcEntry == null){
                LOG.warn("不知道为什么当前pc端和mb 端的数据还没有值,一定是程序出现了bug");
                sb.setLength(0);
                sb.append("当前的环境变量如下: ").append("mbEntry 的值为:").append(mbEntry);
                sb.append("    , pcEntry 的值为: ").append(pcEntry);
                sb.append("当前mb 的key 值是: ").append(mbKey);
                sb.append("当前pc 的key 值是: ").append(pcKey);
                LOG.info(sb.toString());
                continue;

            }
            mbValue = new Double(mbEntry.getValue().toString());
            pcValue = new Double(pcEntry.getValue().toString());
            if(pcValue == 0){
                pcValue = 0.0001;
            }
            double res = mbValue/pcValue;
            res = Math.rint(res * 100) / 100;
            sb = new StringBuilder();
            sb.append("分钟时间戳: ").append(i).append("mb 端与 pc端交易额比值为: ").append(res);
            LOG.info(sb.toString());
            LOG.debug("将数据写入Tair");
            tairOperator.write(ratioKey,res);
            LOG.debug("数据写入成功");
        }
    }

    private void updateAll(){
        //将两个 hashmap 初始化,并更新Tair 中的数据
        LOG.info("执行一次写Tair 工作,该工作将HashMap 中的交易额更新到Tair 中,并清空 HashMap");
        if(pcMiniuteTrades.size()==0 && mbMiniuteTrades.size()==0){
            LOG.info("本次更新pc端 mb 端的交易额为零,数据同步操作将不被执行");
            return ;
        }
        long pcMin = this.currentMaxMiniuteTime;
        long mbMin = this.currentMaxMiniuteTime;

        StringBuilder sb = new StringBuilder();
        sb.append("同步之前pcMiniuteTrades 中的值为: ").append(pcMiniuteTrades);
        LOG.info(sb.toString());
        if(pcMiniuteTrades.size() != 0) {
            List<Map.Entry<Long, Double>> pcSortedLadderSum = getLadderSumFromMap(pcMiniuteTrades);
            sb.setLength(0);
            sb.append("pc端每分钟增加的交易额为: ").append(pcSortedLadderSum);
            LOG.info(sb.toString());
            sb.setLength(0);
            LOG.info("清空pcMiniuteTrades");
            pcMiniuteTrades.clear();
            sb.append("清空之后pcMiniuteTrades 的值为 : ").append(pcMiniuteTrades);
            LOG.info(sb.toString());
            //更新Tair 随着时间变化的总价值
            LOG.info("更新pc 每分钟的总交易额");
            updatePcTrade(pcSortedLadderSum);
            pcMin = pcSortedLadderSum.get(0).getKey();
        }else {
            LOG.info("本次更新 pcMiniuteTrades 没有变化");
        }


        sb.setLength(0);
        sb.append("同步之前mbMiniuteTrades 中的值为: ").append(mbMiniuteTrades);
        LOG.info(sb.toString());
        sb.setLength(0);
        if(mbMiniuteTrades.size() !=0 ) {
            List<Map.Entry<Long, Double>> mbSortedLadderSum = getLadderSumFromMap(mbMiniuteTrades);
            mbMiniuteTrades.clear();
            sb.append("清空mbMiniuteTrades, 清空之后mbMiniuteTrades 的值为: ").append(mbMiniuteTrades);
            LOG.info(sb.toString());
            LOG.info("更新mb 每分钟的总交易额");
            updateMbTrade(mbSortedLadderSum);
            mbMin = mbSortedLadderSum.get(0).getKey();
        }else {
            LOG.info("本次更新 mbMiniuteTrades 没有变化");
        }
        //获得所有时间戳最靠前的时间,目的是为了更新Tair 中存储的交易比例
        long currentMinMiniuteTime = pcMin < mbMin ? pcMin:mbMin;
        sb.setLength(0);
        sb.append("更新 mb-pc 每分钟交易额的比值: 开始时间: ").append(currentMinMiniuteTime).append(".  结束时间: ").append(currentMaxMiniuteTime).
                append("如果两个时间戳不是十位数那么就出现了错误");
        LOG.info(sb.toString());
        updateMbPcRatio(currentMinMiniuteTime,currentMaxMiniuteTime);
    }

}
