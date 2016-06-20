package com.alibaba.middleware.race.model;

import java.io.Serializable;
import java.util.Random;

/**
 * Created by xiyuanbupt on 5/27/16.
 */

/**
 * 在 RocketMq 中的交易消息类型类似于PayMentMessage
 * 对于自定义的消息模型,只要墨香中的各个字段类型和顺序和 PaymentMessage 一样,既可以使用Kryo 反序列化出消息
 */
public class PaymentMessage implements Serializable {

    private static final long serialVersionUID = -4721410670774102273L;

    private long orderId ; //订单ID

    private double payAmount; //金额

    /**
     * Money 来源
     * 0,支付宝
     * 1,红包或代金券
     * 2,银联
     * 3其他
     */
    private short paySource;//来源

    /**
     * 支付平台
     * 0,PC
     * 1,无线
     */
    private short payPlatform;//支付平台

    /**
     * 付款记录创建时间
     */
    private long createTime ;//13 位数,毫秒级的时间戳,初赛要求的时间都是值该时间

    //Kryo 默认需要午餐的构造函数
    public PaymentMessage(){}

    private static Random rand = new Random();

    public static PaymentMessage[] createPayMentMsg(OrderMessage orderMessage){
        PaymentMessage [] list = new PaymentMessage[2];
        for(short i =0;i<2;i++){
            PaymentMessage msg = new PaymentMessage();
            msg.orderId = orderMessage.getOrderId();
            msg.paySource = i;
            msg.payPlatform = (short)(i%2);
            msg.createTime = orderMessage.getCreateTime() + rand.nextInt(100);
            msg.payAmount = 0.0;
            list[i] = msg;
        }
        list[0].payAmount = rand.nextInt((int)(orderMessage.getTotalPrice()/2));
        list[1].payAmount =  orderMessage.getTotalPrice() - list[0].payAmount;
        return list ;
    }

    @Override
    public String toString() {
        return "PaymentMessage{" +
                "orderId=" + orderId +
                ", payAmount=" + payAmount +
                ", paySource=" + paySource +
                ", payPlatform=" + payPlatform +
                ", createTime=" + createTime +
                '}';
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public double getPayAmount() {
        return payAmount;
    }

    public void setPayAmount(double payAmount) {
        this.payAmount = payAmount;
    }

    public short getPaySource() {
        return paySource;
    }

    public void setPaySource(short paySource) {
        this.paySource = paySource;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public short getPayPlatform() {
        return payPlatform;
    }

}
