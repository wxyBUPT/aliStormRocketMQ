package com.alibaba.middleware.race.forLearn;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by xiyuanbupt on 6/9/16.
 * 用于测试线程之间相互操作
 */
public class Test2 {
    private ConcurrentHashMap<Long,Double> testMap = null;

    public Test2(){
        this.testMap = new ConcurrentHashMap<Long, Double>();
        new Thread(new ProduceThread(testMap)).start();
        new Thread(new ConsumerThread(testMap)).start();
        while (true){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e){
                e.printStackTrace();
            }
            for(Map.Entry<Long,Double> entry : testMap.entrySet()){
                System.out.println("Key = " + entry.getKey());
                System.out.println("Value = " + entry.getValue());
            }
        }
    }
    public static void main(String[] args){
        Test2 test = new Test2();
    }
}

class ProduceThread implements Runnable{

    protected ConcurrentHashMap<Long,Double> testMap = null;
    private Random rand;
    public ProduceThread(ConcurrentHashMap map){
        this.testMap = map;
        rand = new Random();
    }

    public void run(){
        while(true){
            try{
                Thread.sleep(1001);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
            for(int i=0;i<100;++i){

                testMap.put((long)i,(double)i + rand.nextDouble() );
            }
        }
    }
}

class ConsumerThread implements Runnable{

    protected  ConcurrentHashMap<Long,Double> testMap = null;
    public ConsumerThread(ConcurrentHashMap map){
        this.testMap = map;
    }

    public void run(){
        System.out.println("我就是一个酱油");
        try{
            Thread.sleep(199);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        System.out.println("我要青空Hash map");
        testMap.clear();
        testMap.put((long)10000000,(double)1000000000);
        for(Map.Entry<Long,Double> entry : testMap.entrySet()) {
            System.out.println("Key = " + entry.getKey());
            System.out.println("Value = " + entry.getValue());
        }
    }
}
