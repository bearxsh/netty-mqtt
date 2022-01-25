package com.bearxsh.test;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.time.LocalDateTime;

public class Producer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        // Specify name server addresses.
        producer.setNamesrvAddr("localhost:9876");
        //Launch the instance.
        producer.start();

        //msg.putUserProperty("MQTT_TOPIC", "ecloud_device_heartbeat");
        for (int i = 0; i < 100; i++) {
            Message msg = new Message("TopicTest2022", "TagA", ("Hello RocketMQ 2022" + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            System.out.println(LocalDateTime.now() + " 发送消息！" + i);
            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }
        producer.shutdown();
    }
}
