package com.incarcloud.rooster.kafka;/**
 * Created by fanbeibei on 2017/6/28.
 */

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * @author Fan Beibei
 * @Description: 使生产者随机提交到不同的分区
 * @date 2017/6/28 11:14
 */
public class RandomPartition implements Partitioner {
    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public void close() {
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value,
                         byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        int partitionNum = 0;
        try {
            partitionNum = Integer.parseInt((String) key);
        } catch (Exception e) {
            partitionNum = key.hashCode();
        }
//        System.out.println("kafkaMessage topic:"+ topic+" |key:"+ key+" |value:"+value);
        return Math.abs(partitionNum % numPartitions);
    }
}
