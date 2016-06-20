package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.tair.TairOperatorImpl;
import com.taobao.tair.DataEntry;
import redis.clients.jedis.Jedis;

import java.util.Set;

/**
 * Created by xiyuanbupt on 6/19/16.
 */
public class ProducerTest {
    public static void main(String[] args){
        //用于测试目的创建了Redis 连接.Redis 中保存了最后的结果数据,以及拥有的所有 key值
        Jedis jedis = new Jedis("10.109.247.29");
        System.out.println("Connection to server sucessfully ");
        Set<String> keys = jedis.smembers(RaceConfig.KeySetForTmTb);

        TairOperatorImpl tairOperator = new TairOperatorImpl(
                RaceConfig.TairConfigServer,
                RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup,
                RaceConfig.TairNamespace
        );
        int count = 0;
        int failCount = 0;
        for(String key : keys){
            count ++;
            Float trades = Float.parseFloat(jedis.get(key));
            System.out.println( key + "   真实的交易额为: " + trades);
            DataEntry entry = tairOperator.get(key);
            try {
                System.out.println("经过storm计算得到的交易额为" + entry.getValue().toString());
            }catch (Exception e){
                System.out.println("tair 中没有这个 key 值的 ,key = " + key);
                failCount ++;
            }
        }
        System.out.println("总共有 " + count  + " 条记录");
        System.out.println("共有: " + failCount + " 条记录没有得到交易额");


        //下面测试 ratio 是否准确
        keys = jedis.smembers(RaceConfig.KeySetForRatio);
        for(String key : keys){
            Float ratio = Float.parseFloat(jedis.get(key));
            System.out.println(key + " 真正的无线端比pc端的交易比例为 : " + ratio);
            DataEntry entry = tairOperator.get(key);
            try {
                System.out.println("经过storm 计算得到的交易壁纸为 " + entry.getValue().toString());
            }catch (Exception e){
                System.out.println("storm 还没有算出相关的值");
            }
        }
        jedis.close();
        tairOperator.close();
        System.out.println("成功关闭两个链接");
    }
}
