package com.zjx.rocketmq_demo.broadcasting;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class BroadcastingConsumerA {

    public static void main(String[] args) throws MQClientException {
        //1.创建defaultMQDefaultConsumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("demo_consumer_broadcasting_group");
        //2.设置nameserv地址
        consumer.setNamesrvAddr("127.0.0.1:9876");
        //默认是集群消费模式，改成广播模式
//        consumer.setMessageModel(MessageModel.BROADCASTING);
        consumer.setConsumeMessageBatchMaxSize(2);
        //3.设置sbuscribe，这里主要是读取的主题信息
        consumer.subscribe("topic_broadcasting_demo", "*");
        //4.创建消息监听messagelistener
        consumer.setMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for(MessageExt msg : list){
                    //获取主题
                    String topic = msg.getTopic();
                    //获取标签
                    String tags = msg.getTags();
                    //获取信息
                    try {
                        String result = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        System.out.println("A消费到消息,topic:" + topic + ",tags:" +  tags + "内容:" + result);
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS.RECONSUME_LATER;
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //开启consumer
        consumer.start();
    }
}
