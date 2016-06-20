package com.alibaba.middleware.race.forLearn;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.tair.TairOperatorImpl;
import com.taobao.tair.DataEntry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by xiyuanbupt on 6/9/16.
 * 用来测试Tair 客户端的
 */
public class TestTair {

    private ConcurrentHashMap<Long,Double> concurrentHashMap = null;
    private ConcurrentHashMap<String,Double> stringDoubleConcurrentHashMap = null;
    private static final String PREFIX = "prefix" ;

    private TairOperatorImpl tairOperator = null;

    private void initTairConnection(){
        tairOperator = new TairOperatorImpl(
                RaceConfig.TairConfigServer,
                RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup,
                RaceConfig.TairNamespace
        );
        return ;
    }
    public TestTair(){
        initTairConnection();
    }

    private void initHashMap(){
        concurrentHashMap = new ConcurrentHashMap<Long, Double>();
        for(long i = 1L;i< 200L;i++){
            concurrentHashMap.put(i,(double)i);
        }
    }

    private void initStringDoubleHashMap(){
        stringDoubleConcurrentHashMap = new ConcurrentHashMap<String, Double>();
        for(long i = 1L;i<200L;i++){
            String key  = PREFIX + i;
            stringDoubleConcurrentHashMap.put(key,(double)i);
        }
    }

    private void testIsStringExpire(){
        System.out.println("将测试使用String 为key 的时候数据是否会失效");
        for(Map.Entry<String,Double> entry: stringDoubleConcurrentHashMap.entrySet()){
            System.out.println("当前的key 值是" + entry.getKey());

            boolean success = tairOperator.write(entry.getKey(),entry.getValue());
            System.out.println("插入的状态是: ");
            System.out.println(success);
            DataEntry entry1 = tairOperator.get(entry.getKey());
            System.out.println("插入的时候获得的value 值是");
            System.out.println(entry1.toString());
        }
        for(Map.Entry<String,Double> entry:stringDoubleConcurrentHashMap.entrySet()){
            System.out.println("获得: key : "+ entry.getKey()+ "的值");
            DataEntry entry1 = tairOperator.get(entry.getKey());
            if(entry1== null){
                System.out.println("同样没有获得到key值的value 值");
            }else {
                System.out.println(entry.toString());
            }
        }
    }

    private void testIsExpire(){
        System.out.println("将hashmap 中的数据写入到 Tair");
        for(Map.Entry<Long,Double> entry : concurrentHashMap.entrySet()){
            tairOperator.write(entry.getKey(),entry.getValue());
            DataEntry entry1 = tairOperator.get(entry.getKey());
            System.out.println("将Tair 中的数据打印,当前的数据为");
            System.out.println(entry1.toString());
        }
        System.out.println("我已经将数据写入到Tair 中了");
        try{
            Thread.sleep(30);
        }catch (Exception e){

        }
        //重新写入数据
        for(long key :concurrentHashMap.keySet()){
            System.out.println("从Tair 中获得key 值的数据");
            System.out.println(key);
            DataEntry entry = tairOperator.get(key);
            if(entry == null){
                System.out.println("key 已经失效了,并不存在");
            }
        }
        System.out.println("第一次执行已经成功");

        try{
            Thread.sleep(3000);
        }catch (Exception e){

        }

        for(long key:concurrentHashMap.keySet()){
            DataEntry entry = tairOperator.get(key);
            if(entry == null){
                System.out.println("key 已经失效了,并不存在");
            }
        }
        System.out.println("所有代码均被执行,并没有发生任何的错误");
        return ;
    }


    public void test(){
        DataEntry entry = tairOperator.get("thekeydosenothavevalue");
        System.out.println(entry);
        boolean success = tairOperator.write("foo","bar");
        entry = tairOperator.get("foo");
        System.out.println("TairClient 一言不合就打日志,我就想看看日志的内容");
        System.out.println(entry.getValue());
        String prefix = "Tair";
        long i = (long)109;
        String key = prefix + i; System.out.println(key); entry = tairOperator.get(key); System.out.println(entry);
        success = tairOperator.write(key,(double)1000);
        entry = tairOperator.get(key);
        System.out.println(entry);
        System.out.println(entry.getValue());
        System.out.println(entry.getValue().getClass().getName());
        System.out.println(entry.getValue());
        double newValue = new Double(entry.getValue().toString()) + 1000;
        System.out.println(newValue);
        //initHashMap();
        //testIsExpire();
        initStringDoubleHashMap();
        testIsStringExpire();

    }

    public static void main(String[] args){
        TestTair testTair = new TestTair();
        testTair.test();
        double a = 100.12645;
        a = (a * 100) / 100;
        System.out.println(a);
        a = Math.rint(a*100)/100;
        System.out.println(a);
    }
}
