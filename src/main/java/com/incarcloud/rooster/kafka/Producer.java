package com.incarcloud.rooster.kafka;/**
 * Created by fanbeibei on 2017/6/28.
 */

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

/**
 * @author Fan Beibei
 * @Description: 描述
 * @date 2017/6/28 14:12
 */
public class Producer {
    private static Logger s_logger = LoggerFactory.getLogger(Producer.class);

    private KafkaProducer<String, String> producer;
    /**
     * 主题
     */
    private String topic;


    private Producer(String topic, Properties props) {
        if (null == topic || null == props) {
            throw new IllegalArgumentException();
        }

        this.topic = topic;


//        props.put("bootstrap.servers", "localhost:9092");


        //以下的配置是不能覆盖的
        props.put("acks", "all");//all表示所有partition都写入才确认，1表示主partition写入即确认，0表示发送后不确认
        props.put("retries", 3);//失败后重发的次数
        props.put("batch.size", 16384);//批量发送的字节数 16K
        props.put("linger.ms", 0);//延迟发送时间,原理就是把原本需要多次发送的小batch，通过引入延时的方式合并成大batch发送，减少了网络传输的压力，从而提升吞吐量。当然，也会引入延时
        props.put("buffer.memory", 33554432);//缓存大小
//        props.put("compression.type", "gzip");//压缩方式，目前支持gzip, snappy和lz4

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");// ByteArraySerializer
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "com.incarcloud.rooster.kafka.RandomPartition");

        producer = new KafkaProducer<>(props);

    }


    /**
     * 发送消息
     *
     * @param content 内容
     * @return offset
     */
    public long send(String content) {

        //key是为了确定丢到哪个partition
        String key = new Random().nextInt(100) + "";
        try {
            Future<RecordMetadata> future =
                    producer.send(new ProducerRecord<String, String>(topic, key, content),
                            new Callback() {
                                @Override
                                public void onCompletion(RecordMetadata metadata, Exception exception) {
                                    s_logger.debug("callback  " + metadata);
                                }
                            });


//        future.get();
            long offset = future.get().offset();
            s_logger.debug("success send ,offset " + offset);

            return offset;
        } catch (Exception e) {
            s_logger.error(e.getMessage());
        }

        return -1;
    }

    /**
     * 批量发送
     *
     * @param contents 内容
     */
    public List<Long> batchSend(List<String> contents) {

        if (null == contents || 0 == contents.size()) {
            throw new IllegalArgumentException();
        }

        List<Long> offsetList = new ArrayList<>(contents.size());

        try {
            for (String content : contents) {
                String key = new Random().nextInt(100) + "";

                Future<RecordMetadata> future =
                        producer.send(new ProducerRecord<String, String>(topic, key, content),
                                new Callback() {
                                    @Override
                                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                                        s_logger.debug("callback  " + metadata);
                                    }
                                });


//        future.get();
                long offset = future.get().offset();
                s_logger.debug("success send ,offset " + offset);
                offsetList.add(offset);

            }

        } catch (Exception e) {
            for (int i = offsetList.size(); i < contents.size(); i++) {
                offsetList.add(-1L);
            }

            s_logger.error(e.getMessage());
        }


        return offsetList;

    }


}
