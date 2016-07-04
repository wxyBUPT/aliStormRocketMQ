package com.alibaba.middleware.race.jstorm.Cache;

import java.io.Serializable;

/**
 * Created by xiyuanbupt on 6/29/16.
 * 用于存储 Order 简单信息
 */
public class OrderSimpleInfo implements Serializable{

    public OrderSimpleInfo(Plat plat,Double totalPrice,Long orderId){
        this.plat = plat;
        this.totalPrice = totalPrice;
        this.calculatedPrice = 0.0;
        this.orderId = orderId;
    }

    private Double totalPrice;
    private Double calculatedPrice;
    private Plat plat;
    private Long orderId;

    public Plat getPlat(){
        return plat;
    }

    public void incrCalculatedPrice(Double price){
        this.calculatedPrice += price;
    }

    public Boolean isFinish(){
        return Math.abs(totalPrice - calculatedPrice) <= 0.000001;
    }

    public Long getOrderId(){
        return orderId;
    }

    public Double getTotalPrice(){
        return totalPrice;
    }

    public Double getCalculatedPrice(){
        return calculatedPrice;
    }

}
