package com.incarcloud.rooster.kafka;/**
 * Created by fanbeibei on 2017/6/28.
 */

import com.incarcloud.rooster.mq.IBigMQ;
import com.incarcloud.rooster.mq.MQException;
import com.incarcloud.rooster.mq.MQMsg;
import com.incarcloud.rooster.mq.MqSendResult;
import com.incarcloud.rooster.util.MQMsgUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * @author Fan Beibei
 * @Description: 发送消息的kafka实现
 * @date 2017/6/28 14:02
 */
public class KafkaMQ implements IBigMQ {
    private static Logger s_logger = LoggerFactory.getLogger(KafkaMQ.class);
    private Producer producer;

    public KafkaMQ(Producer producer) {
        this.producer = producer;
    }


    public MqSendResult post(MQMsg msg) {
        if (null == msg) {
            throw new IllegalArgumentException();
        }

        long offset = producer.send(MQMsgUtil.convertMQMsgToStr(msg));

        if (-1 != offset) {
            return new MqSendResult(null, offset);
        } else {
            return new MqSendResult(new MQException("send error"), offset);
        }
    }

    public List<MqSendResult> post(List<MQMsg> listMsgs) {
        if (null == listMsgs || 0 == listMsgs.size()) {
            throw new IllegalArgumentException("message list is null");
        }

        List<String> contentList = new ArrayList<>(listMsgs.size());
        for (MQMsg msg : listMsgs) {
            contentList.add(MQMsgUtil.convertMQMsgToStr(msg));
        }

        List<MqSendResult> resultList = new ArrayList<>(listMsgs.size());
        List<Long> offsetList = producer.batchSend(contentList);

        for (Long offset : offsetList) {
            if (-1 != offset) {
                resultList.add(new MqSendResult(null, offset));
            } else {
                resultList.add(new MqSendResult(new MQException("send error"), offset));
            }
        }


        return resultList;
    }


    public List<MQMsg> batchReceive(int size) {
        //TODO 接收消息

        return null;
    }

}




