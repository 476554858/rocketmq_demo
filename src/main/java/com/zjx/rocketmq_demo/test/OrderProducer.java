package com.zjx.rocketmq_demo.test;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

public class OrderProducer {
    public static void main(String[] args) throws Exception{
        //1.创建defaultMQProducer
        DefaultMQProducer mqProducer = new DefaultMQProducer("default_producer_groupId");
        //2.设置nameserver地址
        mqProducer.setNamesrvAddr("127.0.0.1:9876");
        //3.开启defaultMQProducer
        mqProducer.start();

        //5.发送消息
        //第1个参数：发送的消息
        //第2个参数：选中指定的消息队列对象(会将所有消息队列传进来)
        //第3个参数: 指定对应的队列下标
        for (int i = 0; i < 6; i++) {
            //4.创建小心message
            Message message = new Message("topic_demo", //主题
                    "Tags",//主要用于过滤消息
                    "keys_" + i, //消息的唯一值
                    ("hello" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult result = mqProducer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    //获取对应的下标
                    Integer index = (Integer) o;
                    return list.get(index);
                }
            }, 1);

            System.out.println(JSONObject.toJSONString(result));
        }
        //6.关闭DefaultMQProducer
        mqProducer.shutdown();
    }
}
