package com.zjx.rocketmq_demo.test;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TransactionProducer {
    public static void main(String[] args) throws Exception{
        //1.创建defaultMQProducer
        TransactionMQProducer mqProducer = new TransactionMQProducer("default_producer_transaction_groupId");

        //2.设置nameserver地址
        mqProducer.setNamesrvAddr("127.0.0.1:9876");
        //3.开启defaultMQProducer
        mqProducer.start();
        //4.创建小心message
        Message message = new Message("topic_transaction_demo", //主题
                "Tags",//主要用于过滤消息
                "keys_1", //消息的唯一值
                "hello trancation".getBytes(RemotingHelper.DEFAULT_CHARSET));

        TransactionListenerImpl transactionListener = new TransactionListenerImpl();
        mqProducer.setTransactionListener(transactionListener);

        ExecutorService threadPool = Executors.newCachedThreadPool();
        mqProducer.setExecutorService(threadPool);

        //5.发送消息
        TransactionSendResult result = mqProducer.sendMessageInTransaction(message, "hello-transaction");
        System.out.println(JSONObject.toJSONString(result));
        //6.关闭DefaultMQProducer
        mqProducer.shutdown();
    }
}
