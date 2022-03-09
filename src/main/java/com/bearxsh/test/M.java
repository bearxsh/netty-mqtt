package com.bearxsh.test;

import com.alibaba.mqtt.server.ServerProducer;
import com.alibaba.mqtt.server.callback.SendCallback;
import com.alibaba.mqtt.server.config.ChannelConfig;
import com.alibaba.mqtt.server.config.ProducerConfig;

import java.nio.charset.StandardCharsets;

public class M {

    public static void main(String[] args) throws Exception {
        /**
         * 此处参数内容为示意。接入点地址，购买实例，且配置完成后即可获取，接入点地址必须填写分配的域名，不得使用 IP 地址直接连接，否则可能会导致客户端异常。
         */
        String domain = "domain";

        /**
         * 使用的协议和端口必须匹配，为5672。
         */
        int port = 5672;

        /**
         * 此处参数内容为示意。MQTT 实例 ID，购买后控制台获取
         */
        String instanceId = "instanceId";

        /**
         * 此处参数内容为示意。账号 accesskey，从账号系统控制台获取
         */
        String accessKey = "accessKey";

        /**
         * 此处参数内容为示意。账号 secretKey，从账号系统控制台获取，仅在Signature鉴权模式下需要设置
         */
        String secretKey = "secretKey";

        /**
         * 此处参数内容为示意。firstTopic是MQTT 消息的一级 topic，需要在控制台申请才能使用。
         * 如果使用了没有申请或者没有被授权的 topic 会导致鉴权失败，MQTT服务端会断开客户端连接。
         */
        String firstTopic = "firstTopic";

        /**
         * 此处参数内容为示意。MQTT支持子级 topic，用来做自定义的过滤，可以填写任何字符串，具体参考https://help.aliyun.com/document_detail/42420.html?spm=a2c4g.11186623.6.544.1ea529cfAO5zV3
         * 需要注意的是，完整的 topic 长度(firstTopic + secondTopic)不得超过128个字符。
         */
        String secondTopic = "secondTopic";

        ChannelConfig channelConfig = new ChannelConfig();
        channelConfig.setDomain(domain);
        channelConfig.setPort(port);
        channelConfig.setInstanceId(instanceId);
        channelConfig.setAccessKey(accessKey);
        channelConfig.setSecretKey(secretKey);

        ServerProducer serverProducer = new ServerProducer(channelConfig, new ProducerConfig());
        serverProducer.start();

        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000);
            String mqttTopic = firstTopic + "/" + secondTopic;
            serverProducer.sendMessage(mqttTopic, ("hello " + i).getBytes(StandardCharsets.UTF_8), new SendCallback() {
                @Override
                public void onSuccess(String msgId) {
                    System.out.println("SendSuccess " + msgId);
                }

                @Override
                public void onFail() {
                    System.out.println("SendFail ");
                }
            });
        }
    }
}