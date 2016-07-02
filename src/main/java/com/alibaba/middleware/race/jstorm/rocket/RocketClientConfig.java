package com.alibaba.middleware.race.jstorm.rocket;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.TimeFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.io.Serializable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by xiyuanbupt on 6/7/16.
 */
public class RocketClientConfig implements Serializable {

    private static final long serialVersionUID = 4157424979688593280L;

    public static final String META_NAMESERVER = "meta.nameserver";

    private final String consumerGroup;

    private final List<String> topics;

    private final String subExpress;

    private int maxFailTimes = DEFAULT_FAIL_TIME;
    public static final int DEFAULT_FAIL_TIME = 5;

    /**
     * Local messages threshold, trigger flow control if excesses
     *
     */
    private int queueSize = DEFAULT_QUEUE_SIZE;
    public static final int DEFAULT_QUEUE_SIZE = 256;

    /**
     * fetch messages size from local queue
     * it is also sending batch size
     *
     */
    private int sendBatchSize = DEFAULT_BATCH_MSG_NUM;
    public static final int DEFAULT_BATCH_MSG_NUM = 32;

    /**
     * pull message size from meta server
     *
     */
    private int pullBatchSize = DEFAULT_BATCH_MSG_NUM;

    /**
     * pull interval(ms) from server for every batch
     *
     */
    private long pullInterval = 0;

    /**
     * pull threads num
     */
    private int pullThreadNum = DEFAULT_PULL_THREAD_NUM;
    public static int DEFAULT_PULL_THREAD_NUM = 4;

    /**
     * Consumer start time Null means start from the last consumption
     * time(CONSUME_FROM_LAST_OFFSET)
     *
     */
    private Date startTimeStamp;

    private Properties peroperties;

    protected RocketClientConfig(String consumerGroup,
                               List<String> topics, String subExpress) {
        this.consumerGroup = consumerGroup;
        this.topics = topics;
        this.subExpress = subExpress;
    }


    public int getMaxFailTimes() {
        return maxFailTimes;
    }

    public void setMaxFailTimes(int maxFailTimes) {
        this.maxFailTimes = maxFailTimes;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public int getSendBatchSize() {
        return sendBatchSize;
    }

    public void setSendBatchSize(int sendBatchSize) {
        this.sendBatchSize = sendBatchSize;
    }

    public int getPullBatchSize() {
        return pullBatchSize;
    }

    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }

    public long getPullInterval() {
        return pullInterval;
    }

    public void setPullInterval(long pullInterval) {
        this.pullInterval = pullInterval;
    }

    public int getPullThreadNum() {
        return pullThreadNum;
    }

    public void setPullThreadNum(int pullThreadNum) {
        this.pullThreadNum = pullThreadNum;
    }

    public Date getStartTimeStamp() {
        return startTimeStamp;
    }

    public void setStartTimeStamp(Date startTimeStamp) {
        this.startTimeStamp = startTimeStamp;
    }

    public Properties getPeroperties() {
        return peroperties;
    }

    public void setPeroperties(Properties peroperties) {
        this.peroperties = peroperties;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }


    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics){
    }

    public String getSubExpress() {
        return subExpress;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this,
                ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
