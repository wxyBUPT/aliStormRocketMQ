package com.alibaba.middleware.race.jstorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtil;
import com.alibaba.middleware.race.jstorm.rocket.RocketTuple;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.tair.TairOperatorImpl;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.taobao.tair.DataEntry;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Created by xiyuanbupt on 6/8/16.
 * 负责 Deserialize tbordermessage 并将需要的信息存储到Tair
 */
public class OrderWriteBolt implements IRichBolt{

    static final Logger LOG = Logger.getLogger(OrderWriteBolt.class);

    protected OutputCollector collector;
    private TairOperatorImpl tairOperator;

    public void prepare(Map stromConf , TopologyContext context,OutputCollector collector){
        LOG.warn("创建了一个OrderWrite Bolt");
        this.collector = collector;
        tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer,
               RaceConfig.TairSalveConfigServer,
               RaceConfig.TairGroup,RaceConfig.TairNamespace);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void execute(Tuple tuple){
        LOG.debug("从rocketMq spout 中接到订单信息");
        RocketTuple rocketTuple = (RocketTuple)tuple.getValue(0);
        List<MessageExt> messageExtList = rocketTuple.getMsgList();
        boolean allSendSucceed = true;
        for(MessageExt messageExt:messageExtList){
            //开始Deserialize 付款信息
            if(messageExt.getTopic().equals(RaceConfig.MqPayTopic)) {
                continue;
            }
            byte[] body = messageExt.getBody();
            if(body.length == 2 && body[0] ==0 && body[1] ==0){
                continue;
            }
            OrderMessage orderMessage = RaceUtil.readKryoObject(OrderMessage.class,body);
            //OrderMessage{orderId=1465371803877,
            // buyerId='buyer2384',
            // productId='product1976',
            // salerId='tb_daler4662',
            // createTime=1465371803978,
            // totalPrice=63129.08}
            long orderId = orderMessage.getOrderId();
            String salerId = orderMessage.getSalerId();
            String platform= salerId.split("_")[0];
            //将平台信息发送到 tair
            Boolean sendSuccessed = tairOperator.write(orderId,platform);
            if(sendSuccessed){

            }else {
                allSendSucceed = false;
            }
            //一下单纯用于测试是否可以在Tair 中获得相关数据
            DataEntry data = (DataEntry) tairOperator.get(orderId);
            String value = (String)data.getValue();
            //System.out.println("下面的数据是从Tair 中获得的");
            //System.out.println("获得的数据是"+ data);
            //System.out.println("获得的value 的值是" + value);

        }
        if(allSendSucceed){
            collector.ack(tuple);
        }else {
            collector.fail(tuple);
        }
    }
}
