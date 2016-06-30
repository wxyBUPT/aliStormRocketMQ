package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * Created by xiyuanbupt on 5/31/16.
 */
public class Producer {
    private static Random rand = new Random();
    private static int count = 800000;

    /**这是一个消息堆积程序,生成的测试消息模型和比赛的时候的消息模型是一样的
     * 这个程序用来线下测试
     */
    static final Jedis jedis = new Jedis("10.109.247.29");

    static final String[] topics = new String[]{RaceConfig.MqTaoboaTradeTopic,RaceConfig.MqTmallTradeTopic};
    static final String[] prefix = new String[]{RaceConfig.prex_taobao,RaceConfig.prex_tmall};
    static final Semaphore semaphore = new Semaphore(0);

    static HashSet<Long> forCountMiniute = new HashSet<Long>();
    //创建一个hashmap,用于保存分钟数 PC端和mb 端交易额
    static HashMap<Long,Double> pcHashMap = new HashMap<Long, Double>();
    static HashMap<Long,Double> mbHashMap = new HashMap<Long, Double>();
    static Long tbOrderCount = 0L;
    static Long tmOrderCount = 0L;
    static Long payMessageCount = 0L;

    public static void main(String[] args)throws MQClientException,InterruptedException{

        DefaultMQProducer producer = new DefaultMQProducer("race_test_data_producer");
        //暂时根据环境变量连接服务器,
        /**
         * Java 启动参数中指定 Name Server 地址
         -Drocketmq.namesrv.addr=192.168.0.1:9876;192.168.0.2:9876
         */
        //如果在代码中写入服务器地址,需要使用下面代码
        //producer.setNamesrvAddr(":9876");
        producer.start();

        //用于测试目的创建了Redis 连接.Redis 中保存了最后的结果数据,以及拥有的所有 key值


        new Thread(new Runnable() {

            //HashSet<Long> forCountMiniute ;
            //HashMap<Long,Double> pcHashMap;
            //HashMap<Long,Double> mbHashMap;

            //public Runnable(HashMap<Long,Double> pcHashMap,HashMap<Long,Double> mbHashMap){
                //this.pcHashMap = pcHashMap;
                //this.mbHashMap = mbHashMap;
            //}

            @Override
            public void run() {
                while(true){
                    try{
                        Thread.sleep(2000);
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                    //下面计算mb/pc 交易额数据
                    HashMap<Long,Double> pcMiniuteTrades = miniuteTrades2miniuteTotalTradesmap(pcHashMap);
                    HashMap<Long,Double> mbMiniuteTrades = miniuteTrades2miniuteTotalTradesmap(mbHashMap);
                    try{
                        for(Map.Entry<Long,Double> entry : pcMiniuteTrades.entrySet()){
                            Long key = entry.getKey();
                            Double ratio = mbMiniuteTrades.get(key)/entry.getValue();
                            String ratioPrefix = RaceConfig.prex_ratio + key;
                            synchronized (jedis) {
                                jedis.sadd(RaceConfig.KeySetForRatio, ratioPrefix);
                                jedis.set(ratioPrefix, ratio.toString());
                                jedis.set(RaceConfig.TaobaoOrderMessageCount,tbOrderCount.toString());
                                jedis.set(RaceConfig.TMOrderMessageCount,tmOrderCount.toString());
                                jedis.set(RaceConfig.PaymentMessageCount,payMessageCount.toString());
                            }
                            System.out.println("分钟 : " + key + "的交易额比例是 : " + ratio);
                            System.out.println("淘宝订单数为: " + tbOrderCount +"  , 天猫订单数为: " + tmOrderCount + "  ,交易订单数为: " + payMessageCount);
                        }
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                    System.out.println("一共产生了 : " + forCountMiniute.size() + "分钟的数据");
                }
            }
        }).start();


        for(int i = 0;i<count;i++){
            try{
                final int platform = rand.nextInt(2);
                final OrderMessage orderMessage = (platform == 0 ? OrderMessage.createTbaoMessage():OrderMessage.createTmallMessage());
                orderMessage.setCreateTime(System.currentTimeMillis());

                byte[] body = RaceUtil.writeKryoObject(orderMessage);

                if(platform ==0 ){
                    tbOrderCount ++;
                }else {
                    tmOrderCount ++;
                }

                Message orderMessageToBroker = new Message(topics[platform],body);

                producer.send(orderMessageToBroker, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        semaphore.release();
                    }

                    @Override
                    public void onException(Throwable throwable) {
                        throwable.printStackTrace();
                    }
                });
                //Send Pay message
                PaymentMessage[] paymentMessages = PaymentMessage.createPayMentMsg(orderMessage);
                double amount = 0;
                for(final PaymentMessage paymentMessage:paymentMessages){
                    payMessageCount ++;
                    int retVal = Double.compare(paymentMessage.getPayAmount(),0);
                    if(retVal < 0 ){
                        throw new RuntimeException("price < 0 !!!!!");
                    }
                    //用于测试目的,将结果同步到 redis中
                    Long time = (paymentMessage.getCreateTime()/1000/60)*60;
                    forCountMiniute.add(time);
                    String key = prefix[platform] + time;
                    synchronized (jedis) {
                        jedis.sadd(RaceConfig.KeySetForTmTb, key);
                        jedis.incrByFloat(key, paymentMessage.getPayAmount());
                    }

                    short plat = paymentMessage.getPayPlatform();
                    if(plat == 0 ){
                        //pc
                        Double trade = pcHashMap.get(time);
                        if(trade == null){
                            trade = 0.0;
                        }
                        trade += paymentMessage.getPayAmount();
                        pcHashMap.put(time,trade);
                    }else if(plat == 1){
                        //无线
                        Double trade = mbHashMap.get(time);
                        if(trade == null){
                            trade = 0.0;
                        }
                        trade += paymentMessage.getPayAmount();
                        mbHashMap.put(time,trade);
                    }

                    if(retVal > 0 ){
                        amount += paymentMessage.getPayAmount();
                        final Message paymentMessageToBroker = new Message(RaceConfig.MqPayTopic,RaceUtil.writeKryoObject(paymentMessage));
                        producer.send(paymentMessageToBroker, new SendCallback() {
                            @Override
                            public void onSuccess(SendResult sendResult) {

                            }

                            @Override
                            public void onException(Throwable throwable) {
                                throwable.printStackTrace();
                            }
                        });
                    }else {
                        //
                    }
                }
                if(Double.compare(amount,orderMessage.getTotalPrice()) != 0){
                    throw new RuntimeException("totalprice is not equal.");
                }
            }catch (Exception e ){
                e.printStackTrace();
            }
        }

        semaphore.acquire(count);
        //所有消息都发送完毕开始发送结束消息
        byte[] zero = new byte[]{0,0};
        Message endMsgTb = new Message(RaceConfig.MqTaoboaTradeTopic,zero);
        Message endMsgTm = new Message(RaceConfig.MqTmallTradeTopic,zero);
        Message endMsgPat = new Message(RaceConfig.MqPayTopic,zero);

        try{
            producer.send(endMsgPat);
            producer.send(endMsgTb);
            producer.send(endMsgTm);
        }catch (Exception e){
            e.printStackTrace();
        }
        producer.shutdown();


    }

    private static HashMap<Long,Double> miniuteTrades2miniuteTotalTradesmap(HashMap<Long,Double> from){
        System.out.println("计算分钟总的交易额");

        HashMap<Long,Double> miniuteTotalTradesmap = new HashMap<Long, Double>(from);
        long minMiniute = getMapMinKey(miniuteTotalTradesmap);
        long maxMiniute = getMapMaxKey(miniuteTotalTradesmap);

        double currentTotalTrades = from.get(minMiniute);

        for(long currentMiniute = minMiniute;currentMiniute<=maxMiniute;currentMiniute+=60){
            if(from.get(currentMiniute) != null){
                currentTotalTrades =+ currentTotalTrades + from.get(currentMiniute);
            }
            miniuteTotalTradesmap.put(currentMiniute,currentTotalTrades);
        }

        return miniuteTotalTradesmap;
    }

    private static  long getMapMinKey(HashMap<Long,Double> hashMap){
        long minVal = 9999999999L;
        for(long key : hashMap.keySet()){
            minVal = minVal<key? minVal:key;
        }
        return minVal;
    }

    private static  long getMapMaxKey(HashMap<Long,Double> hashMap){
        long maxVal = 0L;
        for(long key: hashMap.keySet()){
            maxVal = maxVal > key ? maxVal:key;
        }
        return maxVal;
    }
}
