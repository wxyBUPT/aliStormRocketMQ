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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 7/3/16.
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
    protected static AtomicInteger cacheCount = new AtomicInteger(0);

    //同样将付款信息换粗到
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        platCache = new PlatCache();
        payCache = new PayCache();
        cacheCount.incrementAndGet();
        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true){
                    StringBuilder sb = new StringBuilder();
                    sb.append("Bolt Cache status:  ").append("OrderCache:  ").append(PlatCache.getCacheInfo());
                    sb.append("Payment Cache: ").append(PayCache.getCacheInfo());
                    LOG.info(sb.toString());
                    sb.setLength(0);
                    sb.append("OrderCount is : ").append(orderCount.get());
                    sb.append(",  PayCount is : ").append(payCount.get());
                    sb.append(", CacheCount is : ").append(cacheCount.get());
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
            OrderSimpleInfo orderSimpleInfo = (OrderSimpleInfo) tuple.getValueByField("tuple");
            Long orderId = orderSimpleInfo.getOrderId();
            Plat plat = orderSimpleInfo.getPlat();
            //查看PayCache中是否有订单
            List<PaymentMessage> paymentMessages = payCache.getPaymentMessagesByOrderIdAndRemove(orderId);
            if(paymentMessages != null){
                for(PaymentMessage paymentMessage:paymentMessages){
                    Long createTime = paymentMessage.getCreateTime();
                    Long minuteTime = (createTime/1000/60) * 60;
                    short plat_pc_mb = paymentMessage.getPayPlatform();
                    Double payAmount = paymentMessage.getPayAmount();
                    String plat_tm_tb ;
                    if(plat == Plat.TAOBAO){
                        plat_tm_tb = "tb";
                    }else {
                        plat_tm_tb = "tm";
                    }
                    this.collector.emit(new Values(plat_tm_tb,plat_pc_mb,minuteTime,payAmount));
                    orderSimpleInfo.incrCalculatedPrice(payAmount);
                }
            }
            platCache.addOrderInfoToCache(orderSimpleInfo);

        }else if(type.equals( "pay")){
            payCount.incrementAndGet();
            PaymentMessage paymentMessage = (PaymentMessage)tuple.getValueByField("tuple");
            Long orderId = paymentMessage.getOrderId();
            Double payAmount = paymentMessage.getPayAmount();
            Plat plat = platCache.getPlatAndIncrCalculatedPrice(orderId,payAmount);

            if(plat != null){
                Long createTime = paymentMessage.getCreateTime();
                Long minuteTime = (createTime/1000/60)*60;
                short plat_pc_mb = paymentMessage.getPayPlatform();
                String plat_tm_tb;
                if(plat == Plat.TAOBAO){
                    plat_tm_tb = "tb";
                }else {
                    plat_tm_tb = "tm";
                }
                this.collector.emit(new Values(
                        plat_tm_tb,plat_pc_mb,minuteTime,payAmount
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
                "plat_pc_mb",
                "minuteTime",
                "payAmount"));
    }
}

class PlatCache{
    static private final ConcurrentHashMap<Long,OrderSimpleInfo> cache = new ConcurrentHashMap<Long, OrderSimpleInfo>();

    public PlatCache(){

    }

    public Plat getPlatAndIncrCalculatedPrice(Long orderId,Double price){
        OrderSimpleInfo orderSimpleInfo = cache.get(orderId);
        Plat plat;
        if(orderSimpleInfo!=null){
            orderSimpleInfo.incrCalculatedPrice(price);
            plat = orderSimpleInfo.getPlat();
            if(orderSimpleInfo.isFinish()){
                cache.remove(orderId);
            }
        }else {
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

    public static String getCacheInfo(){
        StringBuilder sb = new StringBuilder();
        sb.append("Plat Cache size is : " + cache.size()).append("   ,");
        return sb.toString();
    }

    public static boolean isEmpty(){
        return cache.size() == 0;
    }
}

//用于缓存付款信息
class PayCache{

    private static final ConcurrentHashMap<Long,List<PaymentMessage>> cache = new ConcurrentHashMap<Long, List<PaymentMessage>>();

    public PayCache(){
    }

    List<PaymentMessage> getPaymentMessagesByOrderId(Long orderId){
        return cache.get(orderId);
    }

    List<PaymentMessage> getPaymentMessagesByOrderIdAndRemove(Long orderId){
        List<PaymentMessage> paymentMessages;
        synchronized (cache) {
            paymentMessages = cache.get(orderId);
            cache.remove(orderId);
        }
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

    public static String getCacheInfo(){
        StringBuilder sb = new StringBuilder();
        sb.append("PayCache size is : " + cache.size()).append("   ,");
        return sb.toString();
    }

    public static boolean isEmpty(){
        return cache.size() == 0;
    }
}
