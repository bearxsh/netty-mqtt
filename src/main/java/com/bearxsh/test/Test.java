package com.bearxsh.test;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.impl.consumer.PullRequest;

public class Test {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("pull_consumer_1");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.start();
        DefaultMQPushConsumerImpl defaultMQPushConsumer = new DefaultMQPushConsumerImpl(consumer, null);
        PullRequest pullRequest = new PullRequest();
        ProcessQueue processQueue = new ProcessQueue();
        pullRequest.setProcessQueue(processQueue);
        defaultMQPushConsumer.pullMessage(pullRequest);

    }
}
