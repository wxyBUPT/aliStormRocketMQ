package com.alibaba.middleware.race.forLearn;


import org.apache.log4j.Logger;

/**
 * Created by xiyuanbupt on 6/16/16.
 */
public class TestLog {
    final static Logger LOG = Logger.getLogger(TestLog.class);

    public static void main(String[] args){
        LOG.debug("这是一条debug 信息");
        LOG.info("这是一条info 信息");
    }
}
