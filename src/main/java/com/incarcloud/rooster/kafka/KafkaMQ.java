package com.incarcloud.rooster.kafka;/**
 * Created by fanbeibei on 2017/6/28.
 */

import com.incarcloud.rooster.mq.IBigMQ;
import com.incarcloud.rooster.mq.MQException;
import com.incarcloud.rooster.mq.MQMsg;
import com.incarcloud.rooster.mq.MqSendResult;
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

        long offset = producer.send(convertMQMsgToStr(msg));

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
            contentList.add(convertMQMsgToStr(msg));
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


    /**
     * 将消息对象转换为字符串
     *
     * @param msg
     * @return
     */
    private String convertMQMsgToStr(MQMsg msg) {
        return msg.getMark() + "->" + msg.getDataB64();
    }

    /**
     * 将字符串转换为消息对象
     *
     * @param s
     * @return
     */
    private MQMsg convertStrToMQMsg(String s){
        if(null == s  || !s.contains("->")){
            return null;
        }


        try {
            MQMsg msg = new MQMsg();
            msg.setMark(s.split("\\->")[0]);
            msg.setData(Base64.getDecoder().decode(s.split("\\->")[1]));

            return msg;
        }catch (Exception e){
            s_logger.error(e.getMessage());
            return  null;
        }
    }

}




