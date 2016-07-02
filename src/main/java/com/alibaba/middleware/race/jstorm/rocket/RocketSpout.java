package com.alibaba.middleware.race.jstorm.rocket;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.jstorm.common.metric.AsmHistogram;
import com.alibaba.jstorm.metric.MetricClient;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.jstorm.Cache.Plat;
import com.alibaba.middleware.race.jstorm.Cache.PlatInfo;
import com.alibaba.middleware.race.jstorm.bolt.PayMessageDeserializeBolt;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import org.apache.log4j.Logger;

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
    protected boolean flowControl;
    protected boolean autoAck;
    protected final static AtomicInteger taobaoOrderCount = new AtomicInteger();
    protected final static AtomicInteger tmOrderCount = new AtomicInteger();
    protected final static AtomicInteger paymessageConsumeSucceedCount = new AtomicInteger();
    protected final static AtomicInteger paymessageConsumeFailCount = new AtomicInteger();

    protected transient LinkedBlockingDeque<RocketTuple> sendingQueue;

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
    public void ack(Object o, List<Object> list) {
        RocketTuple rocketTuple = (RocketTuple) list.get(0);
        finishTuple(rocketTuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("RocketTuple"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void fail(Object o, List<Object> list) {
        RocketTuple rocketTuple = (RocketTuple) list.get(0);
        AtomicInteger failTimes = rocketTuple.getFailureTimes();

        int failNum = failTimes.incrementAndGet();
        if(failNum > rocketClientConfig.getMaxFailTimes()){
            LOG.warn("Message" + rocketTuple.getMq() + "fail times " + failNum);
            finishTuple(rocketTuple);
            return ;
        }

        if(flowControl){
            sendingQueue.offer(rocketTuple);
        }else {
            sendTuple(rocketTuple);
        }
    }

    public void finishTuple(RocketTuple rocketTuple){
        rocketTuple.done();
    }

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.conf = conf;
        this.collector = spoutOutputCollector;
        this.id = topologyContext.getThisComponentId() + ":" + topologyContext.getThisTaskId();
        this.sendingQueue = new LinkedBlockingDeque<RocketTuple>();

        this.flowControl = true;
        this.autoAck = RaceConfig.AutoAck;

        StringBuilder sb = new StringBuilder();
        sb.append("Begin to init MqSpout:").append(id);
        sb.append(",flowControl:").append(flowControl);
        sb.append(", autoAck:").append(autoAck);
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
                            Thread.sleep(10000);
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
        StringBuilder sb = new StringBuilder();
        sb.append("ReportStatuc: ").append("taobaoOrderCount: ").append(taobaoOrderCount.get()).append("  ,");
        sb.append("tmOrderCount: ").append(tmOrderCount.get()).append("   ,");
        sb.append("CacheHashMapCount : ").append(PlatInfo.getInfo()).append("   ,");
        sb.append("QueryCache:  ").append(PayMessageDeserializeBolt.getInfo()).append("   ,");
        LOG.info(sb.toString());
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

    public void sendTuple(RocketTuple rocketTuple){
        rocketTuple.updateEmitMs();
        collector.emit(new Values(rocketTuple),rocketTuple.getCreateMs());
    }

    @Override
    public void nextTuple() {
        RocketTuple rocketTuple = null;
        try{
            rocketTuple = sendingQueue.take();
        }catch (InterruptedException e){

        }
        if(rocketTuple == null){
            return;
        }
        sendTuple(rocketTuple);
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
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
        StringBuilder sb = new StringBuilder();

        int listLen = list.size();

        for(MessageExt messageExt:list){
            byte[] body = messageExt.getBody();
            if (messageExt.getTopic().equals(RaceConfig.MqTaoboaTradeTopic)) {
                OrderMessage orderMessage = RaceUtil.readKryoObject(OrderMessage.class, body);
                consumeTbOrderMessage(orderMessage);
                taobaoOrderCount.incrementAndGet();
                listLen--;
            } else if (messageExt.getTopic().equals(RaceConfig.MqTmallTradeTopic)) {
                OrderMessage orderMessage = RaceUtil.readKryoObject(OrderMessage.class, body);
                consumeTmOrderMessage(orderMessage);
                tmOrderCount.incrementAndGet();
                listLen--;
            }
        }
        if(listLen==0)return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;

        try{
            RocketTuple rocketTuple = new RocketTuple(list,consumeConcurrentlyContext.getMessageQueue());

            if(flowControl){
                sendingQueue.offer(rocketTuple);
            }else {
                sendTuple(rocketTuple);
            }

            if(autoAck){
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }else {
                rocketTuple.waitFinish();
                if(rocketTuple.isSuccess()){
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }else{
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
        }catch (Exception e){
            LOG.error("Fail to emit " + id,e);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    }
    //消费淘宝订单消息
    private void consumeTbOrderMessage(OrderMessage orderMessage){
        Long orderId = orderMessage.getOrderId();
        Double totalPrice = orderMessage.getTotalPrice();
        Plat plat = Plat.TAOBAO;
        PlatInfo.initOrderIdInfo(orderId,plat,totalPrice);

    }

    //消费天猫订单消息
    private void consumeTmOrderMessage(OrderMessage orderMessage){
        Long orderId = orderMessage.getOrderId();
        Double totalPrice = orderMessage.getTotalPrice();
        Plat plat = Plat.TM;
        PlatInfo.initOrderIdInfo(orderId,plat,totalPrice);
    }

    public DefaultMQPushConsumer getConsumer(){
        return consumer;
    }
}
