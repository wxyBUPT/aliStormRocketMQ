package com.alibaba.middleware.race.jstorm.rocket;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.jstorm.Cache.OrderSimpleInfo;
import com.alibaba.middleware.race.jstorm.Cache.Plat;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 6/7/16.
 */
public class RocketSpout implements IRichSpout,
        IAckValueSpout,IFailValueSpout,MessageListenerConcurrently{

    private static final Logger LOG = Logger.getLogger(RocketSpout.class);

    protected RocketClientConfig rocketClientConfig ;

    protected SpoutOutputCollector collector;
    protected transient DefaultMQPushConsumer consumer;

    //下面的conf 是调用open 方法的时候从 topology 传递过来的
    //conf 都是通过submitTopology 中的conf 文件中获得的,而consumer 的初始化又是都从
    //这个配置文件中初始化的,因此为了订阅不同的topic 还要在每个专门的spout 中更改
    protected Map conf;
    protected String id;

    protected transient LinkedBlockingDeque<RocketTuple> paymentSendingQueue;
    protected transient LinkedBlockingDeque<OrderSimpleInfo> orderMessageSendingQueue;

    private final List<String> rocketConsumeTopics;
    private final String rocketConsumeGroup;
    private final String subExp = null;
    //设置consumer 默认的 batchSize
    private int pullBatchSize = 32;

    //只提供一种初始化的方式,所有的spout 都可以使用这份代码
    public RocketSpout(List<String> rocketConsumeTopics,String rocketConsumeGroup){
        //初始化rocket Topic 信息
        this.rocketConsumeTopics = rocketConsumeTopics;
        this.rocketConsumeGroup = rocketConsumeGroup;
    }

    public RocketSpout(List<String> rocketConsumeTopics,String rocketConsumeGroup,int batchSize){
        this.rocketConsumeTopics = rocketConsumeTopics;
        this.rocketConsumeGroup = rocketConsumeGroup;
        this.pullBatchSize = batchSize;
    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("type","tuple"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.conf = conf;
        this.collector = spoutOutputCollector;
        this.id = topologyContext.getThisComponentId() + ":" + topologyContext.getThisTaskId();
        this.paymentSendingQueue= new LinkedBlockingDeque<RocketTuple>();
        this.orderMessageSendingQueue = new LinkedBlockingDeque<OrderSimpleInfo>();


        StringBuilder sb = new StringBuilder();
        sb.append("Begin to init MqSpout:").append(id);
        LOG.info(sb.toString());

        rocketClientConfig = new RocketClientConfig(this.rocketConsumeGroup,this.rocketConsumeTopics
        ,this.subExp);
        rocketClientConfig.setPullBatchSize(this.pullBatchSize);

        //使得consume 订阅相应的topic
        //如下代码代表rocketmq 的consumer 已经被创建,并注册了消费者函数
        try{
            consumer = RocketConsumerFactory.mkInstance(rocketClientConfig,this);
        }catch(Exception e){
            LOG.error("Failed to create Mq Consumer ",e);
            throw new RuntimeException("Failed to create RocketConsumer" + id,e);
        }

        if(consumer == null){
            LOG.error(id + "already exist consumer in current worker, don't need to fetch data");
            //启动新的线程发送没有产生消息的信息
            new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true){
                        try{
                            Thread.sleep(20000);
                        }catch (InterruptedException e){
                            break;
                        }
                        StringBuilder sb = new StringBuilder();
                        sb.append("Only one spout consumer can be run on one process,");
                        sb.append(" but there are multiple spout consumes with the same topic@groupid meta, so the second one");
                        sb.append(id).append(" do nothing");
                        LOG.info(sb.toString());
                    }
                }
            }).start();
        }
        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true){
                    try{
                        Thread.sleep(20000);
                    }catch (Exception e){

                    }
                    report();
                }
            }
        }).start();
    }

    private void report(){

        String info = "Current orderMessageSending queue Size : " + orderMessageSendingQueue.size() + " , Current " +
                "paymentSending queue size is : " + paymentSendingQueue.size();
        LOG.info(info);
    }

    @Override
    public void close() {
        report();
        if(consumer !=null){
            consumer.shutdown();
        }
    }

    @Override
    public void activate() {
        if(consumer != null){
            consumer.resume();
        }
    }

    @Override
    public void deactivate() {
        if(consumer != null){
            consumer.suspend();
        }
    }

    private void sendPayMessage(RocketTuple rocketTuple){
        collector.emit(new Values("pay",rocketTuple));
    }

    private void sendOrderMessage(OrderSimpleInfo orderSimpleInfo){
        collector.emit(new Values("order",orderSimpleInfo));
    }

    @Override
    public void nextTuple() {
        //首先取出订单信息,取出是个订单信息,然后发送出去
        List<OrderSimpleInfo> orderSimpleInfoList = new ArrayList<OrderSimpleInfo>();
        orderMessageSendingQueue.drainTo(orderSimpleInfoList,10);
        for(OrderSimpleInfo orderSimpleInfo:orderSimpleInfoList){
            sendOrderMessage(orderSimpleInfo);
        }

        List<RocketTuple> rocketTuples = new ArrayList<RocketTuple>();
        //付款信息每一次少取一点,因为最好要先消费好订单信息
        paymentSendingQueue.drainTo(rocketTuples,10);
        for(RocketTuple rocketTuple:rocketTuples){
            sendPayMessage(rocketTuple);
        }
    }

    @Override
    public void ack(Object o) {
        LOG.warn("Shouldn't go this function");
    }

    @Override
    public void fail(Object o) {
        LOG.warn("Shouldn't go this function");
    }

    @Override
    public void ack(Object o, List<Object> list) {

    }

    @Override
    public void fail(Object o, List<Object> list) {

    }

    private void consumeOrderMessage(List<MessageExt> list){
        for(MessageExt messageExt:list){
            if(messageExt.getTopic().equals(RaceConfig.MqTaoboaTradeTopic)){
                consumeTaoBaoOrderMessage(messageExt);
            }else if(messageExt.getTopic().equals(RaceConfig.MqTmallTradeTopic)){
                consumeTMOrderMessage(messageExt);
            }else {
                LOG.error("Neither TB Order or TM Order");
            }
        }
    }

    private void consumeTaoBaoOrderMessage(MessageExt messageExt){
        byte[] body = messageExt.getBody();
        OrderMessage orderMessage = RaceUtil.readKryoObject(OrderMessage.class,body);
        Long orderId = orderMessage.getOrderId();
        Double totalPrice = orderMessage.getTotalPrice();
        Plat plat = Plat.TAOBAO;
        OrderSimpleInfo orderSimpleInfo = new OrderSimpleInfo(plat,totalPrice,orderId);
        orderMessageSendingQueue.offer(orderSimpleInfo);

    }

    private void consumeTMOrderMessage(MessageExt messageExt){
        byte[] body = messageExt.getBody();
        OrderMessage orderMessage = RaceUtil.readKryoObject(OrderMessage.class,body);
        Long orderId = orderMessage.getOrderId();
        Double totalPrice = orderMessage.getTotalPrice();
        Plat plat = Plat.TM;
        OrderSimpleInfo orderSimpleInfo = new OrderSimpleInfo(plat,totalPrice,orderId);
        orderMessageSendingQueue.offer(orderSimpleInfo);
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {

        try{
            //如果是订单信息,则将订单信息解序列化,并添加到缓冲队列里面去
            MessageExt messageExt = list.get(0);
            if(!messageExt.getTopic().equals(RaceConfig.MqPayTopic)){
                consumeOrderMessage(list);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        }catch (Exception e){

        }

        try{

            RocketTuple rocketTuple = new RocketTuple(list,consumeConcurrentlyContext.getMessageQueue());
            paymentSendingQueue.offer(rocketTuple);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;

        }catch (Exception e){
            LOG.error("Fail to emit " + id,e);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    }

    public DefaultMQPushConsumer getConsumer(){
        return consumer;
    }
}
