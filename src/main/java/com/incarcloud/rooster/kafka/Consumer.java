package com.incarcloud.rooster.kafka;/**
 * Created by fanbeibei on 2017/7/10.
 */

import com.incarcloud.rooster.mq.MQMsg;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author Fan Beibei
 * @Description: kafka消费者（非线程安全，建议一个线程一个）
 * @date 2017/7/10 11:11
 */
public class Consumer {
    private static Logger s_logger = LoggerFactory.getLogger(Consumer.class);

    private KafkaConsumer<String, byte[]> consumer;


    /**
     * @param props 消费者配置
     */
    public Consumer(Properties props) {
        if (!validConf(props)) {
            throw new IllegalArgumentException();
        }


//        props.put("bootstrap.servers", "localhost:9092");// 该地址是集群的子集，用来探测集群。
//        props.put("group.id", "test");// cousumer的分组id
        props.put("enable.auto.commit", "false");// 自动提交offsets
        // 设置使用最开始的offset偏移量为该group.id的最早。如果不设置，则会是latest即该topic最新一个消息的offset
        // 如果采用latest，消费者只能得道其启动后，生产者生产的消息
        props.put("auto.offset.reset", "earliest");

        props.put("session.timeout.ms", "30000");// Consumer向集群发送自己的心跳，超时则认为Consumer已经死了，kafka会把它的分区分配给其他进程
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");// 反序列化器
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("max.poll.records", "50");

        this.consumer = new KafkaConsumer<String, byte[]>(props);
    }


    protected boolean validConf(Properties props) {
        if (null == props) {
            return false;
        }


        if (null == props.get("bootstrap.servers")) {
            s_logger.error("bootstrap.servers is null !!");
            return false;
        }

        if (null == props.get("group.id")) {
            s_logger.error("group.id is null !!");
            return false;
        }

        return true;
    }

    /**
     * @param topicList 订阅主题
     * @param size      批次大小
     * @return
     */
    public List<MQMsg> batchReceive(List<String> topicList, int size) {
        if (null == topicList || size <= 0) {
            throw new IllegalArgumentException();
        }


        List<MQMsg> msgList = new ArrayList<>(size);

        try {
            ConsumerRecords<String, byte[]> records = consumer.poll(500);
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, byte[]>> partitionRecords = records.records(partition);
                for (ConsumerRecord<String, byte[]> record : partitionRecords) {
                    System.out.println("now consumer the message it's offset is :" + record.offset()
                            + " and the value is :" + record.value());

                    msgList.add(MQMsg.deserializeFromBytes(record.value()));

                }
                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                System.out.println("now commit the partition[ " + partition.partition() + "] offset");
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
            }
        } catch (Exception e) {

        }


        if (0 == msgList.size()) {
            return null;
        }

        return msgList;
    }

    /**
     * 关闭释放消费者
     */
    public void close() {
        consumer.close();
    }

}
