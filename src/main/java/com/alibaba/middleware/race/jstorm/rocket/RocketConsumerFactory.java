package com.alibaba.middleware.race.jstorm.rocket;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by xiyuanbupt on 6/7/16.
 * 产生rocketmq 消费者的代码,注意使用的是环境变量来获得 rocket nameserver 的地址
 */
public class RocketConsumerFactory {

    private static final Logger LOG = Logger.getLogger(RocketConsumerFactory.class);


    public static Map<String,DefaultMQPushConsumer>  consumers = new HashMap<String, DefaultMQPushConsumer>();
    public static synchronized DefaultMQPushConsumer mkInstance(RocketClientConfig config,
                                                                MessageListenerConcurrently listenerConcurrently)throws Exception{
        String topic = config.getTopic();
        String groupId = config.getConsumerGroup();

        String key = topic + "@" + groupId;

        DefaultMQPushConsumer consumer = consumers.get(key);
        if(consumer !=null){
            LOG.info("Consumer of " + key + "has been created,don't recreate it");

            return consumer;
        }
        StringBuilder stringBuilder= new StringBuilder();
        stringBuilder.append("Begin to init rocket client");
        stringBuilder.append(",configuration:").append(config);

        LOG.info(stringBuilder.toString());

        consumer = new DefaultMQPushConsumer(config.getConsumerGroup());

        //下面的代码为获得rocketmq nameServer 地址,默认使用如下方式获得,如果不能获得使用下面的方式
        String ke = "rocketmq.namesrv.addr";
        //通过传递的conf 信息获得Namesrv
        String nameServer = config.getNameServer();

        //在配置中获得NameServer 的地址,因为没有rocketmq 的配置项,所以需要在系统的环境变量中
        //获得NameServer 的地址
        if(nameServer == null){
            nameServer = System.getProperty(ke);
        }

        if(nameServer == null){
            String namekey = "rocketmq.namesrv.domain";

            String value = System.getProperty(namekey);
            nameServer = value;
        }
        if(nameServer == null){
            throw new Exception(
                    "Nameserver dosn't get"
            );
        }

        String instanceName = groupId + "@" + JStormUtils.process_pid();
        LOG.error("current instanceName: " + instanceName);
        consumer.setInstanceName(instanceName);

        //设置从哪里消费信
        //更改从哪里消费逻辑
        if(RaceConfig.isConsumerFromFirstOffset){
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        }else {
            //如果不是从开始消费,那么从当前时间戳开始消费
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
        }
        //设置订阅Topic
        consumer.subscribe(config.getTopic(),config.getSubExpress());
        //设置pushmessageconsumer 的回调函数
        consumer.registerMessageListener(listenerConcurrently);

        consumer.setPullThresholdForQueue(config.getQueueSize());
        consumer.setConsumeMessageBatchMaxSize(config.getSendBatchSize());
        consumer.setPullBatchSize(config.getPullBatchSize());
        consumer.setPullInterval(config.getPullInterval());
        consumer.setConsumeThreadMin(config.getPullThreadNum());
        consumer.setConsumeThreadMax(config.getPullThreadNum());

        consumer.start();
        consumers.put(key,consumer);
        LOG.info("Successfully create " + key  + "consumer, Consumer instanceName : " + consumer.getInstanceName() + ",  ConsumerGroup: " +
        consumer.getConsumerGroup() + ",  Consumer Message Model : " + consumer.getMessageModel() + "  consumeFromWhere : " + consumer.getConsumeFromWhere());

        return consumer;
    }
}
