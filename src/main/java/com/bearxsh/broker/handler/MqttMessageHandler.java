package com.bearxsh.broker.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * see http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 * @author Bearxsh
 * @date 2022/01/21
 */
@ChannelHandler.Sharable
public class MqttMessageHandler extends SimpleChannelInboundHandler<MqttMessage> {
    public static final Map<String/*topic*/, Set<Channel>> subsribeMap = new ConcurrentHashMap<>();

    DefaultMQProducer producer;

    private DefaultMQProducer getProducer() throws MQClientException {
        if (producer == null) {
            producer = new DefaultMQProducer("please_rename_unique_group_name");
            // Specify name server addresses.
            producer.setNamesrvAddr("localhost:9876");
            //Launch the instance.
            producer.start();
        }
        return producer;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage mqttMessage) throws Exception {
        MqttMessageType mqttMessageType = mqttMessage.fixedHeader().messageType();
        MqttFixedHeader mqttFixedHeader;
        switch (mqttMessageType) {
            case PINGREQ:
                ctx.writeAndFlush(MqttMessage.PINGRESP);
                break;
            case PUBLISH:
                MqttQoS mqttQoS = mqttMessage.fixedHeader().qosLevel();
                if (mqttQoS == MqttQoS.AT_MOST_ONCE) {
                    // TODO
                } else if (mqttQoS == MqttQoS.AT_LEAST_ONCE) {
                    System.out.println(LocalDateTime.now() + " receive a message!");

                    MqttPublishMessage mqttPublishMessage = (MqttPublishMessage) mqttMessage;
                    System.out.println(mqttPublishMessage.payload().toString());
                    mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0x02);
                    MqttPubAckMessage mqttPubAckMessage = new MqttPubAckMessage(mqttFixedHeader, MqttMessageIdVariableHeader.from(mqttPublishMessage.variableHeader().packetId()));
                    ctx.writeAndFlush(mqttPubAckMessage);

                    ByteBuf payload = mqttPublishMessage.payload();
                    byte[] arr = new byte[payload.readableBytes()];
                    payload.readBytes(arr);
                    Message msg = new Message("TopicTest", "TagA", arr);
                    System.out.println(msg.toString());
                    //Call send message to deliver message to one of brokers.
                    SendResult sendResult = getProducer().send(msg);
                    System.out.printf("%s%n", sendResult);
                } else if (mqttQoS == MqttQoS.EXACTLY_ONCE) {
                    // 不支持，直接关闭连接
                    ctx.close();
                }
                break;
            case SUBSCRIBE:
                // TODO, 处理
                // 目前仅支持一次订阅一个topic
                MqttSubscribeMessage mqttSubscribeMessage = (MqttSubscribeMessage) mqttMessage;
                String topicName = mqttSubscribeMessage.payload().topicSubscriptions().get(0).topicName();
                Set<Channel> channelSet = subsribeMap.get(topicName);
                if (channelSet == null) {
                    channelSet = new HashSet<>();
                    channelSet.add(ctx.channel());
                    subsribeMap.put(topicName, channelSet);
                } else {
                    channelSet.add(ctx.channel());
                }
                mqttFixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0x03);
                MqttSubAckPayload mqttSubAckPayload = new MqttSubAckPayload(mqttSubscribeMessage.payload().topicSubscriptions().get(0).qualityOfService().value());
                MqttSubAckMessage mqttSubAckMessage = new MqttSubAckMessage(mqttFixedHeader, MqttMessageIdVariableHeader.from(mqttSubscribeMessage.variableHeader().messageId()), mqttSubAckPayload);
                ctx.writeAndFlush(mqttSubAckMessage);
                break;
            case UNSUBSCRIBE:
                MqttUnsubscribeMessage mqttUnsubscribeMessage = (MqttUnsubscribeMessage) mqttMessage;
                mqttFixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0x02);
                MqttUnsubAckMessage mqttUnsubAckMessage = new MqttUnsubAckMessage(mqttFixedHeader, MqttMessageIdVariableHeader.from(mqttUnsubscribeMessage.variableHeader().messageId()));
                ctx.writeAndFlush(mqttUnsubAckMessage);
                break;
            case DISCONNECT:
                // TODO
                break;
            default:
                System.out.println(mqttMessage);
                System.out.println("error: unknown mqtt message!");
                break;
        }
    }
}
