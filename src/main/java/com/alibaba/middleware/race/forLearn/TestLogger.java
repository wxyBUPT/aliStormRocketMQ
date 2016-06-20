package com.alibaba.middleware.race.forLearn;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.tair.TairOperatorImpl;
import org.slf4j.LoggerFactory;

/**
 * Created by xiyuanbupt on 6/10/16.
 */
public class TestLogger {

    public static void main(String[] args){
        System.out.println("这是一条测试命令");
        System.setProperty("org.apache.commons.logging.Log",
                "org.apache.commons.logging.impl.NoOpLog");

        TairOperatorImpl tairOperator = new TairOperatorImpl(
                RaceConfig.TairConfigServer,
                RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup,
                RaceConfig.TairNamespace
        );

    }
}
