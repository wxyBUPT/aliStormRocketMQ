package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.jstorm.Cache.OrderSimpleInfo;
import com.alibaba.middleware.race.jstorm.Cache.Plat;
import com.alibaba.middleware.race.jstorm.Cache.PlatInfo;
import com.alibaba.middleware.race.jstorm.rocket.RocketTuple;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.tair.TairOperatorImpl;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.taobao.tair.DataEntry;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by xiyuanbupt on 6/8/16.
 * 本Bolt 负责发序列化 PaymentMessage 信息,并计算出订单的分钟信息
 * 同时查询Tair 得到平台信息
 * 本部分因为对精度较高,并且是计算结果Bolt 的起始点,所以需要慎重
 * 并且对应的RocketMqSpout 不应该使用 batch  以方便ack 机制
 */
public class PayMessageDeserializeBolt implements IRichBolt{

    static final Logger LOG = Logger.getLogger(PayMessageDeserializeBolt.class);

    protected OutputCollector collector;
    protected static AtomicLong queryLocalCacheSucceedCount = new AtomicLong(0L);
    protected static AtomicLong queryLocalCacheFailCount = new AtomicLong(0L);

    protected LinkedBlockingDeque<PaymentMessageWithFailCount> paymentMessageCacheQueue;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        paymentMessageCacheQueue = new LinkedBlockingDeque<PaymentMessageWithFailCount>();
        LOG.info("PayMessageDeserializeBolt: start nexttupleThread(paymentMessageCacheQueue,outputCollector)");
        new Thread(new NextTupleThread(paymentMessageCacheQueue,outputCollector)).start();
    }

    @Override
    public void execute(Tuple tuple) {
        RocketTuple rocketTuple = (RocketTuple)tuple.getValue(0);
        List<MessageExt> messageExtList = rocketTuple.getMsgList();
        int messageCount = messageExtList.size();
        for(MessageExt messageExt:messageExtList){

            byte[] body = messageExt.getBody();
            if(body.length == 2 && body[0] == 0 && body[1] == 0){
                System.out.println("Got the end signal and do someThing");
                continue;
            }

            PaymentMessage paymentMessage = RaceUtil.readKryoObject(PaymentMessage.class,body);
            //PaymentMessage{orderId=1465113547983,
            // payAmount=1754.0,
            // paySource=0,
            // payPlatform=0,
            // createTime=1465113548255}
            long createTime = paymentMessage.getCreateTime();
            short plat_pc_mb = paymentMessage.getPayPlatform();
            long minuteTime = (createTime / 1000/60)*60;
            long orderId = paymentMessage.getOrderId();
            double payAmount = paymentMessage.getPayAmount();
            //根据订单id 在Tair 中发查询平台信息,因为一个 tuple 被为只有一个payMessage,
            //所以如果查询不成功会执行 collector.fail() 操作
            //System.out.println("收到付款信息,orderId 为 " + orderId);
            //从本地Cache 中获得平台信息
            Plat plat = PlatInfo.getPlatAndIncrCalculatedPrice(orderId,payAmount);
            if(plat != null){
                queryLocalCacheSucceedCount.addAndGet(1);
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
            }else {
                paymentMessageCacheQueue.offer(new PaymentMessageWithFailCount(paymentMessage));
            }
        }
        //尝试清空 paymentMessageCacheQueue
    }

    //从paymentMessageCacheQueus取出一个元素,查询本地cache,如果查询到平台信息,则发送出去,如果查询不到平台信息则放回sendingqueue
    private void nextTuples(){

    }

    @Override
    public void cleanup() {
        LOG.info("Deserialize : " + queryLocalCacheSucceedCount + "pay message has been succeed emit." + queryLocalCacheFailCount + "pay message has not been emit ");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("plat_tm_tb",
                "plat_pc_mb",
                "minuteTime",
                "payAmount"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
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

    //如果失败超过五次,则认为不能
    boolean isFail(){
        return failCount>=50;
    }
}

class NextTupleThread implements Runnable{

    LinkedBlockingDeque<PaymentMessageWithFailCount> sendingQueue;
    OutputCollector collector;

    public NextTupleThread(LinkedBlockingDeque sendingQueue,OutputCollector collector){
        this.sendingQueue = sendingQueue;
        this.collector = collector;
        Thread.currentThread().setPriority(1);

    }

    @Override
    public void run() {
        while(true){
            try {
                PaymentMessageWithFailCount paymentMessageWithFailCount = sendingQueue.take();
                PaymentMessage paymentMessage = paymentMessageWithFailCount.paymentMessage;
                Long orderId = paymentMessage.getOrderId();
                Double payAmount = paymentMessage.getPayAmount();
                Plat plat = PlatInfo.getPlatAndIncrCalculatedPrice(orderId,payAmount);
                if(plat == null){
                    paymentMessageWithFailCount.fail();
                    if(paymentMessageWithFailCount.isFail()){
                        PayMessageDeserializeBolt.queryLocalCacheFailCount.incrementAndGet();
                        StringBuilder sb = new StringBuilder();
                        sb.append("DeserializeLog: get info from  local Cache fail , execute collector's fail function, orderId is : ").append("query local cache Succeed Count: "
                                + PayMessageDeserializeBolt.queryLocalCacheSucceedCount.get()+ "query local Cache FailCount:  "+ PayMessageDeserializeBolt.queryLocalCacheFailCount.get());
                        PayMessageDeserializeBolt.LOG.error(sb.toString());
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
                        PayMessageDeserializeBolt.queryLocalCacheSucceedCount.incrementAndGet();
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
