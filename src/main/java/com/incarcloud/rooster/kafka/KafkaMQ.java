//package com.incarcloud.rooster.kafka;/**
// * Created by fanbeibei on 2017/6/28.
// */
//
//import com.incarcloud.rooster.mq.IBigMQ;
//import com.incarcloud.rooster.mq.MQException;
//import com.incarcloud.rooster.mq.MQMsg;
//import com.incarcloud.rooster.mq.MqSendResult;
//import com.incarcloud.rooster.util.StringUtil;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.UnsupportedEncodingException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Properties;
//
///**
// * @author Fan Beibei
// * @Description: <p>发送消费消息的kafka实现。同一对象只支持发送或消费之一动作，
// * 同时消费和发送请用多个对象。该类中方法均是线程安全</p>
// * @date 2017/6/28 14:02
// */
//public class KafkaMQ implements IBigMQ {
//    private static Logger s_logger = LoggerFactory.getLogger(KafkaMQ.class);
//
//
//    /**
//     * kafka的producer是线程安全的，建议用单例
//     */
//    private Producer producer;
//    /**
//     * 消费者配置，用于创建消费者
//     */
//    private Properties consumerConf;
//
//    /**
//     * 主题
//     */
//    private String topic;
//
//    /**
//     * 只发送消息可用这个构造方法
//     *
//     * @param topic    主题
//     * @param producer 生产者
//     */
//    public KafkaMQ(String topic, Producer producer) {
//        if (StringUtil.isBlank(topic) || null == producer) {
//            throw new IllegalArgumentException();
//        }
//
//        this.topic = topic;
//        this.producer = producer;
//    }
//
//    /**
//     * 只消费消息可用这个构造方法
//     *
//     * @param topic        主题
//     * @param consumerConf 消费者配置，用于创建消费者
//     */
//    public KafkaMQ(String topic, Properties consumerConf) {
//        if (StringUtil.isBlank(topic) || null == consumerConf) {
//            throw new IllegalArgumentException();
//        }
//        this.topic = topic;
//        this.consumerConf = consumerConf;
//    }
//
//
//    @Override
//    public MqSendResult post(MQMsg msg) {
//
//        if (null == msg) {
//            throw new IllegalArgumentException();
//        }
//
//        if (null == producer) {
//            throw new UnsupportedOperationException(" producer is null");
//        }
//
//
//        try {
//            long offset = producer.send(topic, msg.serializeToBytes());
//
//            if (-1 != offset) {
//                return new MqSendResult(null, offset);
//            } else {
//                return new MqSendResult(new MQException("send error"), offset);
//            }
//        } catch (UnsupportedEncodingException e) {
//            s_logger.error("post:  unsupport   UTF-8");
//            return new MqSendResult(new MQException("unsupport   UTF-8"), null);
//
//        }
//    }
//
//    @Override
//    public List<MqSendResult> post(List<MQMsg> listMsgs) {
//
//        if (null == producer) {
//            throw new UnsupportedOperationException(" producer is null");
//        }
//
//        if (null == listMsgs || 0 == listMsgs.size()) {
//            throw new IllegalArgumentException("message list is null");
//        }
//
//        List<byte[]> contentList = new ArrayList<>(listMsgs.size());
//        for (MQMsg msg : listMsgs) {
//            try {
//                contentList.add(msg.serializeToBytes());
//            } catch (UnsupportedEncodingException e) {
//                s_logger.error("post:  unsupport   UTF-8");
//            }
//        }
//
//        List<MqSendResult> resultList = new ArrayList<>(listMsgs.size());
//        List<Long> offsetList = producer.batchSend(topic, contentList);
//
//        for (Long offset : offsetList) {
//            if (-1 != offset) {
//                resultList.add(new MqSendResult(null, offset));
//            } else {
//                resultList.add(new MqSendResult(new MQException("send error"), offset));
//            }
//        }
//
//
//        return resultList;
//    }
//
//    @Override
//    public List<MQMsg> batchReceive(int size) {
//        if (size <= 0) {
//            throw new IllegalArgumentException();
//        }
//
//        if (null == consumerConf) {
//            throw new UnsupportedOperationException(" consumer config  is null");
//        }
//
//        Consumer consumer = currentConsumer();
//
//        return consumer.batchReceive(size);
//    }
//
//    @Override
//    public void close() {
//        if(null != producer){
//            producer.close();
//        }
//    }
//
//
//    @Override
//    public void releaseCurrentConn() {
//        //producer是单例的线程安全模型不需要释放
//
//        if(null != consumerConf) {
//            //释放当前线程的消费者
//            releaseCurrentConsumer();
//        }
//    }
//
//    /**
//     * 由于kafka的consumer是非线程安全，所以用ThreadLocal解决一个线程一个的问题
//     */
//    private ThreadLocal<Consumer> consumerThreadLocal = new ThreadLocal<>();
//
//    /**
//     * 获取当前线程的consumer，不存在则创建一个
//     *
//     * @return
//     */
//    private Consumer currentConsumer() {
//        Consumer consumer = consumerThreadLocal.get();
//        if (null != consumer) {
//            return consumer;
//        }
//
//        consumer = new Consumer(Arrays.asList(topic), consumerConf);
//        consumerThreadLocal.set(consumer);
//
//        return consumer;
//    }
//
//
//    /**
//     * 释放当前的消费者
//     */
//    private void releaseCurrentConsumer() {
//        Consumer consumer = currentConsumer();
//        if (null != consumer) {
//            consumer.close();
//            consumerThreadLocal.set(null);
//        }
//    }
//
//}
//
//
//
//
