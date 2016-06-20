package com.alibaba.middleware.race.rocketmq.learn;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Created by xiyuanbupt on 6/16/16.
 */
public class MultiPullConsumer {

    private static final Logger LOG = Logger.getLogger(MultiPullConsumer.class);

    static final String TB_TOPIC = RaceConfig.MqTaoboaTradeTopic;

    private static final Map offsetTable = new HashMap();

    public static void main(String[] args) throws MQClientException{
        final DefaultMQPullConsumer tbConsumer = new DefaultMQPullConsumer(RaceConfig.MetaConsumerGroup);
        tbConsumer.setNamesrvAddr("10.109.247.29:9876");
        tbConsumer.start();

        try{
            Set<MessageQueue> mqs = tbConsumer.fetchSubscribeMessageQueues(TB_TOPIC);
            LOG.info("mqs.size()" + mqs.size());

            //必须加上此坚挺才能在消费过后,自动回写消费进度

            tbConsumer.registerMessageQueueListener(TB_TOPIC,null);

            //循环每一个队列
            for(MessageQueue mq :mqs){
                LOG.info("Consume message from queus: " + mq + "mqsize " + mqs.size());
                int cnter = 0;

                //每个队列无限循环,分别拉取未消费的消息,知道拉取不到消息为止
                SINGLE_MQ: while (cnter++ < 100){
                    long offset = tbConsumer.fetchConsumeOffset(mq,false);
                    offset = offset<0? 0:offset;
                    System.out.println("消费进度 Offset: " + offset);
                    PullResult result = tbConsumer.pull(mq,null,offset,10);
                    System.out.println("接收到的消息集合: " + result);

                    switch (result.getPullStatus()){
                        case FOUND:
                            if(result.getMsgFoundList() != null){
                                int prSize = result.getMsgFoundList().size();
                                System.out.println("pullResult.getMsgFoundList().size() ======" + prSize);
                                if(prSize != 0){
                                    for(MessageExt me : result.getMsgFoundList()){
                                        System.out.println("pullResult.getMsgFoundList() 消息内容 =====" + new String(me.getBody()));
                                    }
                                }
                            }
                            //获取下一个下标为止
                            offset = result.getNextBeginOffset();

                            //消费完成后更新消费进度
                            tbConsumer.updateConsumeOffset(mq,offset);
                            break ;

                        case NO_MATCHED_MSG:
                            System.out.println("没有匹配的消息");
                            LOG.info("没有匹配的消息");
                            break ;
                        case NO_NEW_MSG:
                            System.out.println("没有未消费的新消息");
                            LOG.info("没有未消费的新消息");
                            //拉取不到新的消息,跳出 SINGLE_MQ 当前队列的循环,开始下一个队列的循环
                            break SINGLE_MQ;

                        case OFFSET_ILLEGAL:
                            System.out.println("小标错误");
                            System.out.println("下标错误");
                            break ;
                        default:
                            break ;
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        final Timer timer = new Timer("TimerThread", true);
        // 定时器延时30秒后，关闭cousumer，因为客户端从首次启动时在1000*10ms即10秒后，后续每5秒定期执行一次（由参数：persistConsumerOffsetInterval控制）向本机及broker端回写记录消费进度，
        // 因此consumer启动后需要延时至少15秒才能执行回写操作，否则下次运行pull方法时，因上次未能及时更新消费进度，程序会重复取出上次消费过的消息重新消费，所以此处延时30秒，留出回写的时间
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                tbConsumer.shutdown();
                // 如果只要这个延迟一次，用cancel方法取消掉．
                this.cancel();
            }
        }, 30000);
    }

    private static void putMessageQueueOffset(MessageQueue mq,long offset){
        offsetTable.put(mq,offset);
    }

    private static long getMessageQueueOffset(MessageQueue mq){
        Long offset = (Long)offsetTable.get(mq);
        if(offset != null){
            return offset;
        }
        return 0;
    }
}
