package com.alibaba.middleware.race.forLearn;

import org.apache.commons.io.output.ByteArrayOutputStream;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by xiyuanbupt on 6/5/16.
 */
public class Test {

    public int pa;
    public Test(int pa){
        this.pa = pa;
    }
    public Test(){

    }

    public static void mapSize(Map map){
        try{
            System.out.println("Index Size: " + map.size());
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(map);
            oos.close();
            System.out.println("Data Size: " + baos.size());
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    final static int MINIUTES_A_DAY = 24 * 60;

    public static void main(String[] args){


        //首先测试 randomin 是否会产生重复的数字
        Random _rand = new Random();
        int _max = 100;
        int val ;
        for(int i =0;i<_max + 12;i++){
            val = _rand.nextInt(_max);
        }
        System.out.println("我产生了超过 _max 的随机数,所以 nextint 是可以产生重复数的");

        //接下来是测试 HashMap 占用内存空间的代码
        Map<Long,Double> counts = new HashMap<Long, Double>();
        //将一天的数据初始换,查看占用多少内存
        mapSize(counts);
        for(long min=0;min<MINIUTES_A_DAY;min++){
            counts.put(min,Double.MAX_VALUE);
        }
        mapSize(counts);
        long start = 0;
        long end = 0;

        System.gc();
        start = Runtime.getRuntime().freeMemory();
        Map<Long,Double> counts2 = new HashMap<Long, Double>();
        for(long max = 0; max < 100000;++max){
            counts2.put(max,Double.MAX_VALUE);
        }
        System.gc();
        end = Runtime.getRuntime().freeMemory();
        System.out.println("一个 100000 个 Long:Double 的HashMap占用内存:" + (end - start));
        mapSize(counts);

        //下面测试从system 中读取环境变量
        String nameServer = "rocketmq.namesrv.domain";
        String value = System.getProperty(nameServer);
        System.out.println("获得nameserver 的地址,地址为"+ value);
        //下面获得一个没有指定的环境变量的值
        value = System.getProperty("wangxiyuan");
        if(value == null){
            System.out.println("没错,当前的环境变量的值为null");
        }else {
            System.out.println("当前环境变量的值不是null");
        }
        Test test = new Test();
    }
}
