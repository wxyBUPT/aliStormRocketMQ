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

            return null;
        }
        StringBuilder stringBuilder= new StringBuilder();
        stringBuilder.append("Begin to init rocket client");
        stringBuilder.append(",configuration:").append(config);

        LOG.info(stringBuilder.toString());

        //下面的代码为获得rocketmq nameServer 地址,默认使用如下方式获得,如果不能获得使用下面的方式
        //通过传递的conf 信息获得Namesrv
        String nameServer = config.getNameServer();

        if(nameServer != null){
            String raceNameKey = "rocketmq.namesrv.addr";
            String commonNameKey = "rocketmq.namesrv.domain";

            //如果在配置文件中显示的指定了nameserver 并且在 raceNamekey 中没有显示的指定nameserver,那么设置
            String value = System.getProperty(raceNameKey);
            if(value == null){
                System.setProperty(raceNameKey,nameServer);
            }
            value = System.getProperty(commonNameKey);
            if(value == null){
                System.setProperty(commonNameKey,nameServer);
            }
        }

        //在配置中获得NameServer 的地址,因为没有rocketmq 的配置项,所以需要在系统的环境变量中
        //获得NameServer 的地址
        //if(nameServer == null){
        //    nameServer = System.getProperty(ke);
        //}


        //if(nameServer == null){
        //    throw new Exception(
        //            "Nameserver dosn't get"
        //    );
        //}

        consumer = new DefaultMQPushConsumer(config.getConsumerGroup());
        String instanceName = topic + "@" + groupId + "@" + JStormUtils.process_pid();
        LOG.error("current instanceName: " + instanceName);
        consumer.setInstanceName(instanceName);

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
        if(RaceConfig.isConsumerFromFirstOffset){
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        }else {
            //如果不是从开始消费,那么从当前时间戳开始消费
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
        }
        consumer.start();
        consumers.put(key,consumer);
        LOG.info("Successfully create " + key  + "consumer, Consumer instanceName : " + consumer.getInstanceName() + ",  ConsumerGroup: " +
        consumer.getConsumerGroup() + ",  Consumer Message Model : " + consumer.getMessageModel() + "  consumeFromWhere : " + consumer.getConsumeFromWhere());

        return consumer;
    }
}
