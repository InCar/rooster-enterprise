package com.incarcloud.rooster.mq;

import com.incarcloud.rooster.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by Kong on 2018/1/11.
 */
public class KafkaMQ implements IBigSuperMQ {

    private static Logger s_logger = LoggerFactory.getLogger(KafkaMQ.class);

    ConcurrentMap<String,KProducer> producerConcurrentMap = new ConcurrentHashMap<>() ;

    ConcurrentMap<String,KConsumer> consumerConcurrentMap = new ConcurrentHashMap<>() ;


    /**
     * 只发送消息可用这个构造方法
     *
     * @param topic    主题
     * @param producer 生产者
     */
    public KafkaMQ setProducer(String topic,KProducer producer){
        if (StringUtil.isBlank(topic) || null == producer) {
            throw new IllegalArgumentException();
        }
        producerConcurrentMap.put(topic,producer) ;
        return this ;
    }

    /**
     * 只消费消息可用这个构造方法
     *
     * @param topic      主题
     * @param props  消费者
     */
    public KafkaMQ setConsumer(String topic,Properties props){
        if (StringUtil.isBlank(topic) || null == props) {
            throw new IllegalArgumentException();
        }
        KConsumer consumer = new KConsumer(topic,props) ;
        consumerConcurrentMap.put(topic,consumer) ;
        return this ;
    }

    @Override
    public MqSendResult post(String topic,byte[] data) {
        if (null == data) {
            throw new IllegalArgumentException();
        }
        if (StringUtil.isBlank(topic)){
            throw new UnsupportedOperationException(" topic is null");
        }

        KProducer producer = producerConcurrentMap.get(topic) ;

        if (null == producer) {
            throw new UnsupportedOperationException(" producer is null");
        }

        long offset = producer.send(topic, data);

        if (-1 != offset) {
            return new MqSendResult(null, offset);
        } else {
            return new MqSendResult(new MQException("send error"), offset);
        }
    }

    @Override
    public List<MqSendResult> post(String topic ,List<byte[]> datas) {
        if (StringUtil.isBlank(topic)){
            throw new UnsupportedOperationException(" topic is null");
        }

        KProducer producer = producerConcurrentMap.get(topic) ;

        if (null == producer) {
            throw new UnsupportedOperationException(" producer is null");
        }

        if (null == datas || 0 == datas.size()) {
            throw new IllegalArgumentException("message list is null");
        }

        List<MqSendResult> resultList = new ArrayList<>(datas.size());

        List<Long> offsetList = producer.batchSend(topic, datas);

        for (Long offset : offsetList) {
            if (-1 != offset) {
                resultList.add(new MqSendResult(null, offset));
            } else {
                resultList.add(new MqSendResult(new MQException("send error"), offset));
            }
        }

        return resultList;
    }

    /**
     * 由于kafka的consumer是非线程安全，所以用ThreadLocal解决一个线程一个的问题
     */
    private ThreadLocal<KConsumer> consumerThreadLocal = new ThreadLocal<>();

    @Override
    public List<byte[]> batchReceive(String topic, int size) {
        if (size <= 0) {
            throw new IllegalArgumentException();
        }
        if (StringUtil.isBlank(topic)){
            throw new UnsupportedOperationException(" topic is null");
        }

        KConsumer kConsumer = consumerConcurrentMap.get(topic) ;

        if (null == kConsumer) {
            throw new UnsupportedOperationException(" consumer config  is null");
        }

        KConsumer consumer = currentConsumer(topic);

        return consumer.batchReceive(size);
    }

    @Override
    public void releaseCurrentConn(String topic) {
        KConsumer consumer = consumerConcurrentMap.get(topic) ;
        //producer是单例的线程安全模型不需要释放
        if(null != consumer) {
            //释放当前线程的消费者
            releaseCurrentConsumer(topic);
        }
    }

    @Override
    public void close() {
        producerConcurrentMap.forEach((key,value)->{
            if(null != value){
                value.close();
            }
        });
    }

    /**
     * 获取当前线程的consumer，不存在则创建一个
     *
     * @return
     */
    private KConsumer currentConsumer(String topic) {
        KConsumer consumer = consumerThreadLocal.get();
        if (null != consumer) {
            return consumer;
        }
        consumer = consumerConcurrentMap.get(topic) ;

        if (null == consumer) {
            throw new UnsupportedOperationException(" consumer config  is null");
        }

        consumerThreadLocal.set(consumer);

        return consumer;
    }

    /**
     * 释放当前的消费者
     */
    private void releaseCurrentConsumer(String topic) {
        KConsumer consumer = currentConsumer(topic);
        if (null != consumer) {
            consumer.close();
            consumerThreadLocal.set(null);
        }
    }
}
