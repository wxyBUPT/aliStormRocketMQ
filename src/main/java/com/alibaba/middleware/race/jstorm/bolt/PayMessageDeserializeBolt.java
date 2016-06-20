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
import com.alibaba.middleware.race.jstorm.rocket.RocketTuple;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.tair.TairOperatorImpl;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.taobao.tair.DataEntry;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Created by xiyuanbupt on 6/8/16.
 * 本Bolt 负责发序列化 PaymentMessage 信息,并计算出订单的分钟信息
 * 同时查询Tair 得到平台信息
 * 本部分因为对精度较高,并且是计算结果Bolt 的起始点,所以需要慎重
 * 并且对应的RocketMqSpout 不应该使用 batch  以方便ack 机制
 */
public class PayMessageDeserializeBolt implements IRichBolt{

    private static final Logger LOG = Logger.getLogger(PayMessageDeserializeBolt.class);

    protected OutputCollector collector;
    private TairOperatorImpl tairOperator;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        tairOperator = new TairOperatorImpl(
                RaceConfig.TairConfigServer,
                RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup,
                RaceConfig.TairNamespace
        );
    }

    @Override
    public void execute(Tuple tuple) {
        RocketTuple rocketTuple = (RocketTuple)tuple.getValue(0);
        List<MessageExt> messageExtList = rocketTuple.getMsgList();
        for(MessageExt messageExt:messageExtList){
            //Deserialize 付款信息
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
            long minuteTime = createTime / 1000;
            long orderId = paymentMessage.getOrderId();
            double payAmount = paymentMessage.getPayAmount();
            //根据订单id 在Tair 中发查询平台信息,因为一个 tuple 被为只有一个payMessage,
            //所以如果查询不成功会执行 collector.fail() 操作
            //System.out.println("收到付款信息,orderId 为 " + orderId);
            DataEntry entry =  tairOperator.get(orderId);
            if(entry != null){
                //emit 计算并emit 数据
                String plat_tm_tb = (String)entry.getValue();
                //System.out.println("从Tair 查询到数据");
                StringBuilder sb = new StringBuilder();
                sb.append("从Tair 中查询到信息,并emit 相关内容,plat_pc_mb : ").append(plat_pc_mb);
                sb.append("  ,plat_tm_tb:  ").append(plat_tm_tb).append("   , minuteTime").append(minuteTime).append("   , payAmount: ").append(payAmount);
                LOG.debug(sb.toString());
                this.collector.emit(new Values(
                        plat_tm_tb,
                        plat_pc_mb,
                        minuteTime,
                        payAmount
                        ));
                this.collector.ack(tuple);
            }else {
                StringBuilder sb = new StringBuilder();
                sb.append("没有从Tair 中查询到,执行 collector 的fail操作,orderId 为: ").append(orderId);
                LOG.info(sb.toString());
                break;
            }
        }
    }

    @Override
    public void cleanup() {

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
