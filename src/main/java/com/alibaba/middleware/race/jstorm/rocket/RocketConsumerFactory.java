package com.alibaba.middleware.race.jstorm.rocket;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xiyuanbupt on 6/7/16.
 * 产生rocketmq 消费者的代码,注意使用的是环境变量来获得 rocket nameserver 的地址
 */
public class RocketConsumerFactory {

    private static final Logger LOG = Logger.getLogger(RocketConsumerFactory.class);


    public static Map<String,DefaultMQPushConsumer>  consumers = new HashMap<String, DefaultMQPushConsumer>();
    public static synchronized DefaultMQPushConsumer mkInstance(RocketClientConfig config,
                                                                MessageListenerConcurrently listenerConcurrently)throws Exception{
        List<String> topics = config.getTopics();
        String groupId = config.getConsumerGroup();

        String key = topics + "@" + groupId;

        DefaultMQPushConsumer consumer = consumers.get(key);
        if(consumer !=null){
            LOG.info("Consumer of " + key + "has been created,don't recreate it");
            return null;
        }

        consumer = new DefaultMQPushConsumer(config.getConsumerGroup());
        String instanceName = groupId + "@" + JStormUtils.process_pid();
        LOG.info("current instanceName: " + instanceName);
        consumer.setInstanceName(instanceName);

        //设置订阅Topic
        for(String topic:config.getTopics()) {
            consumer.subscribe(topic, config.getSubExpress());
        }
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
