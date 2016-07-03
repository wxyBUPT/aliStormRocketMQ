package com.alibaba.middleware.race.jstorm.Cache;

/**
 * Created by xiyuanbupt on 6/29/16.
 * 用于存储 Order 简单信息
 */
public class OrderSimpleInfo {

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
        if(Math.abs(totalPrice - calculatedPrice) < 0.5){
            return true;
        }else {
            return false;
        }
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
