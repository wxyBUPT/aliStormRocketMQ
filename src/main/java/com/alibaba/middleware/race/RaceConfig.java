package com.alibaba.middleware.race;

import java.io.Serializable;

/**
 * Created by xiyuanbupt on 5/31/16.
 */
public class RaceConfig implements Serializable{

    //tair key 的前缀
    public static String prex_tmall = "platformTmall_41084gxo4o_";
    public static String prex_taobao = "platformTaobao_41084gxo4o_";
    public static String prex_ratio = "ratio_41084gxo4o_";

    //jstorm/rocketMq/tair/kafka/redis 的集群配置信息,这些配置信息在提交代码之前应该修改
    public static String JstormTopologyName = "41084gxo4o";

    public static String MetaConsumerGroup = "41084gxo4o";
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaoboaTradeTopic = "MiddlewareRaceTestData_TBOrder";

    public static String TairConfigServer = "10.101.72.127:5198";
    public static String TairSalveConfigServer = "10.101.72.128:5198";
    public static String TairGroup = "group_tianchi";
    public static Integer TairNamespace = 25395;

    //public static String TairConfigServer = "115.28.93.106:5198";
    //public static String TairSalveConfigServer =null;
    //public static String TairGroup = "group_1";
    //public static Integer TairNamespace = 0;

    //测试环境使用配置
    public static boolean isConsumerFromFirstOffset = true;

    //ReportToTairThread 多久会同步程序中的内存数据到 Tair,每次同步内存数据会被清除
    //数据单位是毫秒,当前是每半分钟同步一次数据
    public static Integer ReportRatioToTairInterval = 30000;
    //tb,tm Trade 同步到 Tair 的时间间隔.
    public static Integer ReportTbTmTradeInterval = 25000;
    public static boolean AutoAck = true;

    //下面的参数是用于本地测试的
    //redis 中保存pc mb 每分钟交易额 的所有key 值的key
    public static String KeySetForTmTb= "2jj";
    public static String KeySetForRatio = "3jj";
    public static String TaobaoOrderMessageCount = "4jj";
    public static String TMOrderMessageCount = "5jj";
    public static String PaymentMessageCount = "6jj";
}
