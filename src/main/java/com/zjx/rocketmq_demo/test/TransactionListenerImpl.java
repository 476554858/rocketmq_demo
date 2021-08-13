package com.zjx.rocketmq_demo.test;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class TransactionListenerImpl implements TransactionListener{
    //存储对应事务的状态信息
    private Map<String, Integer> localTrans = new ConcurrentHashMap<>();

    //执行本地事务
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        //获取事务ID
        String transactionId = message.getTransactionId();
        //0.执行中,未知状态 1.执行成功 2.执行失败
        localTrans.put(transactionId, 0);
        //执行本地事务
        try {
            System.out.println("正在执行本地事务=====");
            TimeUnit.SECONDS.sleep(100);
            System.out.println("本地事务执行成功=====");
            localTrans.put(transactionId, 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
            localTrans.put(transactionId, 2);
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        return LocalTransactionState.COMMIT_MESSAGE;
    }
    //执行事务回查
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        String transactionId = messageExt.getTransactionId();
        Integer status = localTrans.get(transactionId);
        System.out.println("开始回查事务状态," + ",事务ID：" + transactionId +",事务状态:" + status);
        switch (status){
            case 0:
                return LocalTransactionState.UNKNOW;
            case 1:
                return LocalTransactionState.COMMIT_MESSAGE;
            case 3:
                return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        return LocalTransactionState.ROLLBACK_MESSAGE;
    }
}
