package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.jstorm.Cache.OrderSimpleInfo;
import com.alibaba.middleware.race.jstorm.Cache.Plat;
import com.alibaba.middleware.race.model.PaymentMessage;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 7/3/16.
 * 使用storm 的fields grouping 可以保证相同的orderId 缓存到同一个Cache bolt 中去
 */
public class CacheBolt extends BaseRichBolt{

    protected static Logger LOG = Logger.getLogger(CacheBolt.class);

    private OutputCollector collector;

    //使用缓存存储先关信息
    //用于缓存订单信息
    private static PlatCache platCache ;
    private static PayCache payCache;
    protected static AtomicInteger orderCount = new AtomicInteger(0);
    protected static AtomicInteger payCount = new AtomicInteger(0);

    //同样将付款信息换粗到
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        platCache = new PlatCache();
        payCache = new PayCache();
        //new Thread(new ClearThread(collector,payCache,platCache)).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true){
                    StringBuilder sb = new StringBuilder();
                    sb.append("Bolt Cache status:  ").append("OrderCache:  ").append(platCache.getCacheInfo());
                    sb.append("Payment Cache: ").append(payCache.getCacheInfo());
                    LOG.info(sb.toString());
                    sb.setLength(0);
                    sb.append("OrderCount is : ").append(orderCount.get());
                    sb.append(",  PayCount is : ").append(payCount.get());
                    LOG.info(sb.toString());
                    try {
                        Thread.sleep(20000);
                    }catch (Exception e){

                    }
                }
            }
        }).start();
    }

    @Override
    public void execute(Tuple tuple) {
        //接到的tuple 如下 new Values("pay",paymentMessage)
        // Values("order",orderSimpleInfo)
        // Fields("type","tuple")
        String type = tuple.getStringByField("type");
        if(type.equals("order")){
            orderCount.incrementAndGet();
            OrderSimpleInfo orderSimpleInfo = (OrderSimpleInfo) tuple.getValueByField("class");
            Long orderId = orderSimpleInfo.getOrderId();
            Plat plat = orderSimpleInfo.getPlat();
            //查看PayCache中是否有订单
            List<PaymentMessage> paymentMessages = payCache.getPaymentMessagesByOrderIdAndRemove(orderId);
            if(paymentMessages != null){
                for(PaymentMessage paymentMessage:paymentMessages){
                    Long createTime = paymentMessage.getCreateTime();
                    Long minuteTime = (createTime/1000/60) * 60;
                    Double payAmount = paymentMessage.getPayAmount();
                    String plat_tm_tb ;
                    if(plat == Plat.TAOBAO){
                        plat_tm_tb = "tb";
                    }else {
                        plat_tm_tb = "tm";
                    }
                    this.collector.emit(new Values(plat_tm_tb,minuteTime,payAmount));
                    orderSimpleInfo.incrCalculatedPrice(payAmount);
                }
            }
            if(!orderSimpleInfo.isFinish()) {
                platCache.addOrderInfoToCache(orderSimpleInfo);
            }

        }else if(type.equals( "pay")){
            payCount.incrementAndGet();
            PaymentMessage paymentMessage = (PaymentMessage)tuple.getValueByField("class");
            Long orderId = paymentMessage.getOrderId();
            Double payAmount = paymentMessage.getPayAmount();
            Plat plat = platCache.getPlatAndIncrCalculatedPrice(orderId,payAmount);
            if(plat != null){
                Long createTime = paymentMessage.getCreateTime();
                Long minuteTime = (createTime/1000/60)*60;
                String plat_tm_tb;
                if(plat == Plat.TAOBAO){
                    plat_tm_tb = "tb";
                }else {
                    plat_tm_tb = "tm";
                }
                this.collector.emit(new Values(
                        plat_tm_tb,minuteTime,payAmount
                ));
            }else {
                payCache.cachePaymentMessage(paymentMessage);
            }
        }else {
            LOG.error("Neither order message nor pay message");
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("plat_tm_tb",
                "minuteTime",
                "payAmount"));
    }
}

class PlatCache{
    private ConcurrentHashMap<Long,OrderSimpleInfo> cache = new ConcurrentHashMap<Long, OrderSimpleInfo>();

    public PlatCache(){
        cache = new ConcurrentHashMap<Long, OrderSimpleInfo>();
    }

    synchronized public Plat getPlatAndIncrCalculatedPrice(Long orderId,Double price){
        OrderSimpleInfo orderSimpleInfo = cache.get(orderId);
        Plat plat;
        if (orderSimpleInfo != null) {
            orderSimpleInfo.incrCalculatedPrice(price);
            plat = orderSimpleInfo.getPlat();
            if (orderSimpleInfo.isFinish()) {
                cache.remove(orderId);
            }
        } else {
            plat = null;
        }
        return plat;
    }

    public void addOrderInfoToCache(OrderSimpleInfo orderSimpleInfo){
        Long orderId = orderSimpleInfo.getOrderId();
        cache.put(orderId,orderSimpleInfo);
    }

    @Override
    public String toString(){
        return cache.toString();
    }

    public String getCacheInfo(){
        StringBuilder sb = new StringBuilder();
        sb.append("Plat Cache size is : " + cache.size()).append("   ,");
        return sb.toString();
    }

    public boolean isEmpty(){
        return cache.size() == 0;
    }

    boolean isOrderCome(Long orderId){
        return cache.keySet().contains(orderId);
    }
}

//用于缓存付款信息
class PayCache{

    private ConcurrentHashMap<Long,List<PaymentMessage>> cache = new ConcurrentHashMap<Long, List<PaymentMessage>>();

    public PayCache(){
        cache = new ConcurrentHashMap<Long, List<PaymentMessage>>();
    }

    List<PaymentMessage> getPaymentMessagesByOrderId(Long orderId){
        return cache.get(orderId);
    }

    synchronized List<PaymentMessage> getPaymentMessagesByOrderIdAndRemove(Long orderId){
        List<PaymentMessage> paymentMessages;
        paymentMessages = cache.get(orderId);
        cache.remove(orderId);
        return paymentMessages;
    }

    void cachePaymentMessage(PaymentMessage paymentMessage){
        Long orderId = paymentMessage.getOrderId();
        List<PaymentMessage> paymentMessages = cache.get(orderId);
        if(paymentMessages==null){
            paymentMessages = new ArrayList<PaymentMessage>();
            cache.put(orderId,paymentMessages);
        }
        paymentMessages.add(paymentMessage);
    }

    @Override
    public String toString(){
        return cache.toString();
    }

    public String getCacheInfo(){
        StringBuilder sb = new StringBuilder();
        sb.append("PayCache size is : " + cache.size()).append("   ,");
        return sb.toString();
    }

    public boolean isEmpty(){
        return cache.size() == 0;
    }

    public Set<Long> getPayOrderIdSet(){
        return cache.keySet();
    }
}

class ClearThread implements Runnable{

    OutputCollector collector;
    PayCache payCache;
    PlatCache platCache;

    ClearThread(OutputCollector collector,PayCache payCache,PlatCache platCache){
        this.collector = collector;
        this.payCache = payCache;
        this.platCache = platCache;
    }

    @Override
    public void run() {
        while(true){
            Set<Long> orderIdSet = payCache.getPayOrderIdSet();
            for(Long orderId:orderIdSet){
                if(platCache.isOrderCome(orderId)){
                    List<PaymentMessage> paymentMessages = payCache.getPaymentMessagesByOrderIdAndRemove(orderId);
                    for(PaymentMessage paymentMessage:paymentMessages){
                        Double payAmount= paymentMessage.getPayAmount();
                        Plat plat = platCache.getPlatAndIncrCalculatedPrice(orderId,payAmount);
                        String plat_tm_tb ;
                        if(plat==Plat.TAOBAO){
                            plat_tm_tb = "tb";
                        }else {
                            plat_tm_tb = "tm";
                        }
                        Long createTime = paymentMessage.getCreateTime();
                        Long minuteTime = (createTime/1000/60)*60;
                        this.collector.emit(new Values(
                                plat_tm_tb,minuteTime,payAmount
                        ));
                    }
                }
            }
        }
    }
}
