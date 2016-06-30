package com.alibaba.middleware.race.jstorm.Cache;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by xiyuanbupt on 6/29/16.
 * 用于存储 订单ID:平台信息
 * 当订单ID被全部付款之后删除该订单ID的信息
 */
public class PlatInfo {

    public static ConcurrentHashMap<Long,OrderSimpleInfo> platDB = new ConcurrentHashMap<Long, OrderSimpleInfo>();

    public static void initOrderIdInfo(Long orderId,Plat plat,Double totalPrice){
        OrderSimpleInfo orderSimpleInfo = new OrderSimpleInfo(plat,totalPrice);
        platDB.put(orderId,orderSimpleInfo);
    }

    public static Plat getPlatAndIncrCalculatedPrice(Long orderId,Double price){
        //显示的等1毫秒,等待订单信息被存储
        OrderSimpleInfo orderSimpleInfo = platDB.get(orderId);
        Plat plat;
        if(orderSimpleInfo !=null){
            orderSimpleInfo.incrCalculatedPrice(price);
            plat = orderSimpleInfo.getPlat();
            if(orderSimpleInfo.isFinish() ){
                platDB.remove(orderId);
            }else {

            }
        }else {
            plat = null;
        }
        return plat;
    }

    //如果没有查询到,则注册异步回调函数

}
