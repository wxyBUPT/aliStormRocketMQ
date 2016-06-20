package com.alibaba.middleware.race.tair;

import ch.qos.logback.classic.Level;
import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Created by xiyuanbupt on 6/7/16.
 */
public class TairOperatorImpl {

    //更改Tair 的日志级别为info
    static {
        ch.qos.logback.classic.Logger rootLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(
                ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME

        );
        rootLogger.setLevel(Level.toLevel("info"));
    }

    private DefaultTairManager tairManager ;
    private int namespace;

    public TairOperatorImpl(
            String masterConfigServer,
            String slaveConfigServer,
            String groupName,
            int namespace
    ){
        List<String> confServer = new ArrayList<String>();
        confServer.add(masterConfigServer);

        tairManager = new DefaultTairManager();
        tairManager.setConfigServerList(confServer);

        tairManager.setGroupName(groupName);
        tairManager.init();
        this.namespace = namespace;
    }

    public boolean write(Serializable key,Serializable value){
        ResultCode rc = tairManager.put(namespace,key,value);
        if(rc.isSuccess()){
            return true;
        }else {
            return false;
        }
    }

    public DataEntry get(Serializable key){
        Result<DataEntry> result = tairManager.get(namespace,key);
        if(result.isSuccess()){
            DataEntry entry = result.getValue();
            return entry;
        }
        else {
            return null;
        }
    }

    public boolean remove(Serializable key){
        return false;
    }

    public boolean close(){
        tairManager.close();
        return true;
    }

    public static void main(String[] args){
        System.out.println("就是用来测试这个Tair 客户端是否可用");
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer,
                RaceConfig.TairSalveConfigServer,RaceConfig.TairGroup,RaceConfig.TairNamespace);
        tairOperator.write("foo","bar");
        Object ob =  tairOperator.get("foo");
        System.out.println(ob);
    }

}