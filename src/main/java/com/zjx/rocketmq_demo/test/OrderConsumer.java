package com.zjx.rocketmq_demo.test;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class OrderConsumer {

    public static void main(String[] args) throws MQClientException {
        //1.创建defaultMQDefaultConsumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("demo_order_consumer_group");
        //2.设置nameserv地址
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.setConsumeMessageBatchMaxSize(2);
        //3.设置sbuscribe，这里主要是读取的主题信息
        consumer.subscribe("topic_demo", "*");
        //4.创建消息监听messagelistener
        consumer.setMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                for(MessageExt msg : list){
                    //获取主题
                    String topic = msg.getTopic();
                    //获取标签
                    String tags = msg.getTags();
                    //获取信息
                    try {
                        String result = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        System.out.println("消费道德消息,topic:" + topic + ",tags:" +  tags + "内容:" + result);
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    }
                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        //开启consumer
        consumer.start();
    }
}
