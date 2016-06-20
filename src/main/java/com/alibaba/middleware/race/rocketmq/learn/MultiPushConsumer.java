package com.alibaba.middleware.race.rocketmq.learn;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * Created by xiyuanbupt on 6/16/16.
 */
public class MultiPushConsumer {

    public static void main(String[] args) throws MQClientException,InterruptedException{
        MQPushConsumer tmConsumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);

        //MQPushConsumer tbConsumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);

        tmConsumer.subscribe(RaceConfig.MqTaoboaTradeTopic,null);

        //tbConsumer.subscribe(RaceConfig.MqTmallTradeTopic,"*");

        tmConsumer.registerMessageListener(
                new MessageListenerConcurrently() {
                    @Override
                    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                        System.out.println("从天猫的消息队列中消费一条信息");
                        System.out.println("Receive New Messages : " + list);
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                }
        );

        //tbConsumer.registerMessageListener(
        //        new MessageListenerConcurrently() {
        //            @Override
        //            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
        //                System.out.println("从淘宝的消息队列中消费了一条信息");
        //                System.out.println("Receive New Messages : " + list);
        //                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        //            }
        //        }
        //);

        System.out.println("Consumer Start ");
        while (true){
            Thread.sleep(1000);
        }
    }
}
