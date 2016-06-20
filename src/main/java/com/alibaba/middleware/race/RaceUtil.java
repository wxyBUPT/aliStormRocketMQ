package com.alibaba.middleware.race;

import com.alibaba.middleware.race.model.OrderMessage;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Created by xiyuanbupt on 5/31/16.
 */
public class RaceUtil {
    /**
     * 消息是使用 Kryo序列化之后堆积到相关的mq 中的(RocketMq,Kafka),需要使用metaQ获取消息
     * 发序列化出消息类型,只要消息模型的定义类似于 OrderMessage 和PaymentMessage 即可
     */

    public static byte[] writeKryoObject(Object object){
        Output output = new Output(1024);
        Kryo kryo = new Kryo();
        kryo.writeObject(output,object);
        output.flush();
        output.close();
        byte[] ret = output.toBytes();
        output.clear();
        return ret;
    }

    public static <T> T readKryoObject(Class<T> tClass,byte[] bytes){
        Kryo kryo = new Kryo();
        Input input = new Input(bytes);
        input.close();
        T ret = kryo.readObject(input,tClass);
        return ret;
    }

    public static void main(String[] args){
        OrderMessage orderMessage = OrderMessage.createTmallMessage();
        orderMessage.setCreateTime(System.currentTimeMillis());

        byte[] body = RaceUtil.writeKryoObject(orderMessage);
        System.out.println(body);

        OrderMessage orderMessage1 = RaceUtil.readKryoObject(OrderMessage.class,body);
        System.out.println(orderMessage1);

        System.out.println(orderMessage1.getOrderId());
    }
}
