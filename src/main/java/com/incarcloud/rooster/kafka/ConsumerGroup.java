package com.incarcloud.rooster.kafka;/**
 * Created by fanbeibei on 2017/6/28.
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Fan Beibei
 * @Description: 消费者组
 * @date 2017/6/28 10:15
 */
public class ConsumerGroup {
    private static Logger s_logger = LoggerFactory.getLogger(ConsumerGroup.class);

    /**
     * 执行定时监控运行状态的线程池
     */
    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);

    /**
     * 默认的consumer数，和partition数相等
     */
    private static final int DEFAULT_SIZE = 3;

    /**
     * 组ID，唯一,自动生成
     */
    private String groupId = "ConsumerGroup" + Calendar.getInstance().getTimeInMillis();
    /**
     * 订阅的主题
     */
    private List<String> topics;

    /**
     * 一组包含的consumer数量,不能超过partition数量，建议设置能被partition数整除的数字
     */
    private int size;


    /**
     * 消费者配置信息
     */
    private Properties props;


    private ThreadGroup threadGroup = new ThreadGroup(groupId + "-threadGroup");

    /**
     * @param topics 订阅的主题
     * @param props  consumer配置
     */
    public ConsumerGroup(List<String> topics, Properties props) {
        this(topics, props, DEFAULT_SIZE);
    }

    /**
     * @param topics 订阅的主题
     * @param props  consumer配置
     * @param size   consumer数量
     */
    public ConsumerGroup(List<String> topics, Properties props, int size) {
        if (null == topics || 0 == topics.size() || null == props || size <= 0) {
            throw new IllegalArgumentException();
        }
        this.topics = topics;


        this.props = props;


//        this.props.put("bootstrap.servers", "localhost:9092");// 该地址是集群的子集，用来探测集群。

        //以下是不能被重置的配置
        this.props.setProperty("group.id", groupId);
        this.props.put("enable.auto.commit", "false");// 自动提交offsets
        // 设置使用最开始的offset偏移量为该group.id的最早。如果不设置，则会是latest即该topic最新一个消息的offset
        // 如果采用latest，消费者只能得道其启动后，生产者生产的消息
        this.props.put("auto.offset.reset", "earliest");
        this.props.put("session.timeout.ms", "20000");// Consumer向集群发送自己的心跳，超时则认为Consumer已经死了，kafka会把它的分区分配给其他进程
        this.props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");// 反序列化器
        this.props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        this.props.put("max.poll.records", "50");//每次poll最大纪录数
    }


    /**
     * 创建consumer
     *
     * @param props
     * @return
     */
    private KafkaConsumer<String, String> createConsumer(Properties props) {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        return consumer;
    }


    /**
     * consumer组启动
     */
    public void start() {
        //启动订阅线程
        for (int i = 0; i < size; i++) {
            new Thread(threadGroup, new SubscribeTask(createConsumer(props))).start();
        }


        //监控线程
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {

            }
        },5,20, TimeUnit.SECONDS);
    }


    /**
     * 订阅任务
     */
    private class SubscribeTask implements Runnable {
        private KafkaConsumer<String, String> consumer;

        public SubscribeTask(KafkaConsumer<String, String> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            consumer.subscribe(topics);// 订阅的topic,可以多个
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        s_logger.debug("now consumer the message it's offset is :" + record.offset()
                                + " and the value is :" + record.value());
                    }
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();

                    s_logger.debug("now commit the partition[ " + partition.partition() + "] offset");

                    //TODO 解析保存到habse

                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                }
            }
        }
    }


}
