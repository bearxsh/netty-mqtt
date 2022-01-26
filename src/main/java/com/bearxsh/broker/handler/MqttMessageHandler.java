package com.bearxsh.broker.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * see http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
 * @author Bearxsh
 * @date 2022/01/21
 */
@ChannelHandler.Sharable
public class MqttMessageHandler extends SimpleChannelInboundHandler<MqttMessage> {

    /**
     * 一个topic只能有一个终端订阅
     */
    public static final Map<String/*topic*/, Channel> SUBSCRIBE_MAP = new ConcurrentHashMap<>();

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
                    // 不支持，直接关闭连接
                    ctx.close();
                } else if (mqttQoS == MqttQoS.AT_LEAST_ONCE) {
                    System.out.println(LocalDateTime.now() + " receive a message!");
                    System.out.println(mqttMessage);

                    MqttPublishMessage mqttPublishMessage = (MqttPublishMessage) mqttMessage;
                    ByteBuf payload = mqttPublishMessage.payload();
                    byte[] arr = new byte[payload.readableBytes()];
                    payload.readBytes(arr);

                    Message msg = new Message(mqttPublishMessage.variableHeader().topicName(), arr);
                    String clientId = MqttServerHandler.CHANNEL_CLIENT_MAP.get(ctx.channel());
                    System.out.println(LocalDateTime.now() + " clientId=" + clientId);
                    SendResult sendResult = getProducer().send(msg, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            String clientId = (String) arg;
                            int index = clientId.hashCode() % mqs.size();
                            System.out.println("send to queue: " + Math.abs(index));
                            return mqs.get(Math.abs(index));
                        }
                    }, clientId);
                    if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                        System.out.println(mqttPublishMessage.payload().toString());
                        mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0x02);
                        MqttPubAckMessage mqttPubAckMessage = new MqttPubAckMessage(mqttFixedHeader, MqttMessageIdVariableHeader.from(mqttPublishMessage.variableHeader().packetId()));
                        ctx.writeAndFlush(mqttPubAckMessage);
                    }
                } else if (mqttQoS == MqttQoS.EXACTLY_ONCE) {
                    // 不支持，直接关闭连接
                    ctx.close();
                }
                break;
            case SUBSCRIBE:
                // TODO, 处理
                // 目前仅支持一次订阅一个topic，一个topic只能被一个终端订阅
                MqttSubscribeMessage mqttSubscribeMessage = (MqttSubscribeMessage) mqttMessage;
                String topicName = mqttSubscribeMessage.payload().topicSubscriptions().get(0).topicName();
                Channel channel = SUBSCRIBE_MAP.get(topicName);
                if (channel == null || !channel.isActive()) {
                    SUBSCRIBE_MAP.put(topicName, ctx.channel());
                } else {
                    System.err.println("已经被订阅，关闭连接");
                    ctx.close();
                    return;
                }
                mqttFixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0x03);
                MqttSubAckPayload mqttSubAckPayload = new MqttSubAckPayload(mqttSubscribeMessage.payload().topicSubscriptions().get(0).qualityOfService().value());
                MqttSubAckMessage mqttSubAckMessage = new MqttSubAckMessage(mqttFixedHeader, MqttMessageIdVariableHeader.from(mqttSubscribeMessage.variableHeader().messageId()), mqttSubAckPayload);
                ctx.writeAndFlush(mqttSubAckMessage);
                break;
            case UNSUBSCRIBE:
                // TODO 取消订阅
                MqttUnsubscribeMessage mqttUnsubscribeMessage = (MqttUnsubscribeMessage) mqttMessage;
                mqttFixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0x02);
                MqttUnsubAckMessage mqttUnsubAckMessage = new MqttUnsubAckMessage(mqttFixedHeader, MqttMessageIdVariableHeader.from(mqttUnsubscribeMessage.variableHeader().messageId()));
                ctx.writeAndFlush(mqttUnsubAckMessage);
                break;
            case DISCONNECT:
                ctx.close();
                break;
            default:
                System.out.println(mqttMessage);
                System.out.println("error: unknown mqtt message!");
                break;
        }
    }
}
