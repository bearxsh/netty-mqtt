package com.bearxsh.test;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.time.LocalDateTime;

public class MqttAsyncProducer {
    public static void main(String[] args) {

        String topic        = "MQTTExamples";
        String content      = "Message from MqttPublishSample";
        int qos             = 1;
        String broker       = "tcp://localhost:1883";
        String clientId     = "JavaSample";
        MemoryPersistence persistence = new MemoryPersistence();

        try {
            MqttAsyncClient sampleClient = new MqttAsyncClient(broker, clientId, persistence);
            // 设置客户端发送超时时间，防止无限阻塞。但是感觉没用啊，依旧阻塞
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            System.out.println("Connecting to broker: "+broker);
            sampleClient.connect(connOpts);
            System.out.println("Connected");
            System.out.println(LocalDateTime.now() + " Publishing message: "+content);
            MqttMessage message = new MqttMessage(content.getBytes());
            message.setQos(qos);
            while (!sampleClient.isConnected()) {

            }

            IMqttDeliveryToken publish = sampleClient.publish(topic, message);
            publish.waitForCompletion();
            System.out.println("Message published");
            sampleClient.disconnect();
            System.out.println("Disconnected");
            System.exit(0);
        } catch(MqttException me) {
            me.printStackTrace();
        }
    }
}
