package com.alibaba.middleware.race.forLearn;

import redis.clients.jedis.Jedis;

/**
 * Created by xiyuanbupt on 6/19/16.
 */
public class UseRedis {
    public static void main(String[] args){
        Jedis jedis = new Jedis("10.109.247.29");
        System.out.println("Connection to server sucessfully ");
        jedis.set("foo","bar");
        System.out.println(jedis.get("foo"));
        Long a = 1000L;
        String prefix = "tm";
        String b = prefix + a;
        System.out.println(b);
        Double c = null;
        Double d = 1.0;
        double e = d/d;
        System.out.println(e);
    }
}
