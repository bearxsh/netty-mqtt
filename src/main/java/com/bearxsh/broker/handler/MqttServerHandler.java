package com.bearxsh.broker.handler;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.*;

/**
 * see http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718033
 * @author Bearxsh
 * @date 2022/01/21
 */
@ChannelHandler.Sharable
public class MqttServerHandler extends SimpleChannelInboundHandler<MqttConnectMessage> {

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, MqttConnectMessage mqttConnectMessage) {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.AT_MOST_ONCE, false, 0x02);
        MqttConnAckVariableHeader mqttConnAckVariableHeader = new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false);
        MqttConnAckMessage mqttConnAckMessage = new MqttConnAckMessage(mqttFixedHeader, mqttConnAckVariableHeader);
        channelHandlerContext.writeAndFlush(mqttConnAckMessage);
    }
}
