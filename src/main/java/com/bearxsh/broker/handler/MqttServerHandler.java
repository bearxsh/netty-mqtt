package com.bearxsh.broker.handler;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * see http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718033
 * @author Bearxsh
 * @date 2022/01/21
 */
@ChannelHandler.Sharable
public class MqttServerHandler extends SimpleChannelInboundHandler<MqttConnectMessage> {

    public static final Map<Channel, String/*clientId*/> CHANNEL_CLIENT_MAP = new ConcurrentHashMap<>();

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, MqttConnectMessage mqttConnectMessage) {
        // TODO 保存channel到clientId的映射，clientId不能重复，如果重复则踢掉旧连接
        String clientId = mqttConnectMessage.payload().clientIdentifier();
        CHANNEL_CLIENT_MAP.put(channelHandlerContext.channel(), clientId);
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0x02);
        MqttConnAckVariableHeader mqttConnAckVariableHeader = new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false);
        MqttConnAckMessage mqttConnAckMessage = new MqttConnAckMessage(mqttFixedHeader, mqttConnAckVariableHeader);
        channelHandlerContext.writeAndFlush(mqttConnAckMessage);
    }
}
