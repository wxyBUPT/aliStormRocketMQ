package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.jstorm.Cache.OrderSimpleInfo;
import com.alibaba.middleware.race.jstorm.Cache.Plat;
import com.alibaba.middleware.race.jstorm.rocket.RocketTuple;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by xiyuanbupt on 7/3/16.
 */
public class CacheBolt extends BaseRichBolt{

    protected static Logger LOG = Logger.getLogger(CacheBolt.class);

    private OutputCollector collector;
    protected static AtomicLong queryLocalCacheSucceedCount = new AtomicLong(0L);
    protected static AtomicLong queryLocalCacheFailCount = new AtomicLong(0L);

    //使用缓存存储先关信息
    //用于缓存订单信息
    private PlatCache platCache ;

    private LinkedBlockingDeque<PaymentMessageWithFailCount> paymentMessageCacheQueue;

    //同样将付款信息换粗到
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        platCache = new PlatCache();
        this.paymentMessageCacheQueue = new LinkedBlockingDeque<PaymentMessageWithFailCount>();
        //需要开启一定数量查询平台信息的线程,开启两个线程
        new Thread(new NextTupleThread(
                paymentMessageCacheQueue,platCache,outputCollector
        )).start();
        new Thread(new NextTupleThread(
                paymentMessageCacheQueue,platCache,outputCollector
        )).start();
    }

    @Override
    public void execute(Tuple tuple) {
        //接到的tuple 如下 new Values("pay",rocketTuple)
        // Values("order",orderSimpleInfo)
        // Fields("type","tuple")
        String type = tuple.getStringByField("type");
        if(type=="order"){
            OrderSimpleInfo orderSimpleInfo = (OrderSimpleInfo) tuple.getValueByField("tuple");
            platCache.addOrderInfoToCache(orderSimpleInfo);
        }else if(type == "pay"){
            RocketTuple rocketTuple = (RocketTuple) tuple.getValueByField("tuple");
            List<MessageExt> messageExtList = rocketTuple.getMsgList();
            for(MessageExt messageExt:messageExtList){
                byte[] body = messageExt.getBody();
                if(body.length == 2 && body[0] == 0 && body[1] == 0){
                    continue;
                }
                PaymentMessage paymentMessage = RaceUtil.readKryoObject(PaymentMessage.class,body);
                Long orderId = paymentMessage.getOrderId();
                Double payAmount = paymentMessage.getPayAmount();
                Plat plat = platCache.getPlatAndIncrCalculatedPrice(orderId,payAmount);
                if(plat != null){
                    queryLocalCacheSucceedCount.addAndGet(1);
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
                    paymentMessageCacheQueue.offer(new PaymentMessageWithFailCount(paymentMessage));
                }
            }
        }else {
            LOG.error("Neither order message nor pay message");
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}

class PlatCache{
    private static ConcurrentHashMap<Long,OrderSimpleInfo> cache ;

    public PlatCache(){
        cache = new ConcurrentHashMap<Long, OrderSimpleInfo>();
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
}

//用于缓存暂时没有查询到平台信息的付款信息
class PaymentMessageWithFailCount{

    PaymentMessage paymentMessage;
    int failCount ;
    public PaymentMessageWithFailCount(PaymentMessage paymentMessage){
        this.paymentMessage = paymentMessage;
        this.failCount = 0;
    }

    void fail(){failCount++;}

    //如果失败的超过一定的次数,则认为查询失败,则认为不能
    boolean isFail(){
        return failCount>=100;
    }
}

class NextTupleThread implements Runnable{

    LinkedBlockingDeque<PaymentMessageWithFailCount> sendingQueue;
    OutputCollector collector;
    PlatCache platCache;


    public NextTupleThread(LinkedBlockingDeque sendingQueue,PlatCache platCache,OutputCollector collector){
        this.sendingQueue = sendingQueue;
        this.collector = collector;
        this.platCache = platCache;
    }

    @Override
    public void run() {
        try{
            //由于里面的消息都是未被查询到的信息,所以显示的让本线程延迟一段时间执行
            Thread.sleep(2000);
        }catch (Exception e){

        }

        while(true){
            try {
                PaymentMessageWithFailCount paymentMessageWithFailCount = sendingQueue.take();
                PaymentMessage paymentMessage = paymentMessageWithFailCount.paymentMessage;
                Long orderId = paymentMessage.getOrderId();
                Double payAmount = paymentMessage.getPayAmount();
                Plat plat = platCache.getPlatAndIncrCalculatedPrice(orderId,payAmount);
                if(plat == null){
                    paymentMessageWithFailCount.fail();
                    if(paymentMessageWithFailCount.isFail()){
                        CacheBolt.queryLocalCacheFailCount.incrementAndGet();
                        StringBuilder sb = new StringBuilder();
                        sb.append("QueryCache Log : get info from  local Cache fail , execute collector's fail function," +
                                " orderId is : ").append("query local cache Succeed Count: "
                                + CacheBolt.queryLocalCacheSucceedCount.get()+ "query local Cache FailCount:  "+ CacheBolt.queryLocalCacheFailCount.get());
                        CacheBolt.LOG.debug(sb.toString());
                    }else {
                        sendingQueue.offer(paymentMessageWithFailCount);
                    }
                }
                else{
                    long createTime = paymentMessage.getCreateTime();
                    short plat_pc_mb = paymentMessage.getPayPlatform();
                    long minuteTime = (createTime / 1000/60)*60;
                    //根据订单id 在Tair 中发查询平台信息,因为一个 tuple 被为只有一个payMessage,
                    //所以如果查询不成功会执行 collector.fail() 操作
                    //System.out.println("收到付款信息,orderId 为 " + orderId);
                    //从本地Cache 中获得平台信息
                    CacheBolt.queryLocalCacheSucceedCount.incrementAndGet();
                    //emit 计算并emit 数据
                    String plat_tm_tb ;
                    if(plat == Plat.TAOBAO){
                        plat_tm_tb = "tb";
                    }else {
                        plat_tm_tb = "tm";
                    }
                    this.collector.emit(new Values(
                            plat_tm_tb,
                            plat_pc_mb,
                            minuteTime,
                            payAmount
                    ));
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}