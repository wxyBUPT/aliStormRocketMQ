package com.alibaba.middleware.race.jstorm.rocket;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xiyuanbupt on 6/7/16.
 */
public class RocketTuple implements Serializable{

    protected final List<MessageExt> msgList;
    protected final MessageQueue mq;

    protected final AtomicInteger failureTimes;
    protected final long createMs;
    protected long emitMs;

    protected transient CountDownLatch latch;
    protected transient boolean isSuccess;

    public RocketTuple(List<MessageExt> msgList,MessageQueue mq){
        this.msgList = msgList;
        this.mq = mq;

        this.failureTimes = new AtomicInteger(0);
        this.createMs = System.currentTimeMillis();

        this.latch = new CountDownLatch(1);
        this.isSuccess = false;
    }

    public AtomicInteger getFailureTimes(){
        return failureTimes;
    }

    public long getCreateMs(){
        return createMs;
    }

    public void updateEmitMs(){
        this.emitMs = System.currentTimeMillis();
    }

    public long getEmitMs(){
        return this.emitMs;
    }

    public List<MessageExt> getMsgList(){
        return msgList;
    }

    public MessageQueue getMq(){
        return mq;
    }

    public boolean waitFinish() throws InterruptedException{
        return latch.await(4, TimeUnit.HOURS);
    }

    public void done(){
        isSuccess = true;
        latch.countDown();
    }

    public void fail(){
        isSuccess = false;
        latch.countDown();
    }

    public boolean isSuccess(){
        return isSuccess;
    }

    @Override
    public String toString(){
        return ToStringBuilder.reflectionToString(
                this, ToStringStyle.SHORT_PREFIX_STYLE
        );
    }
}
