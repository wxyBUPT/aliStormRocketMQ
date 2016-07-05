package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.tair.TairOperatorImpl;
import redis.clients.jedis.Jedis;

import java.util.Set;

/**
 * Created by xiyuanbupt on 7/4/16.
 */
public class ClearResult {

    public static void main(String[] args){
        Jedis jedis = new Jedis("10.109.247.29");
        Set<String> keys = jedis.smembers(RaceConfig.KeySetForTmTb);

        TairOperatorImpl tairOperator = TairOperatorImpl.getInstance();
        for(String key:keys){
            tairOperator.write(key,0.0);
        }
        keys = jedis.smembers(RaceConfig.KeySetForRatio);
        for(String key:keys){
            tairOperator.write(key,0.0);
        }
        tairOperator.close();
    }
}
