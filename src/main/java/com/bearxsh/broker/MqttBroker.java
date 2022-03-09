package com.bearxsh.broker;

import com.bearxsh.broker.handler.MqttMessageHandler;
import com.bearxsh.broker.handler.MqttServerHandler;
import io.netty.bootstrap.ServerBootstrap;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;

import io.netty.buffer.Unpooled;
import io.netty.channel.*;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import org.apache.rocketmq.client.log.ClientLogger;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.time.LocalDateTime;
import java.util.List;


/**
 * @author bearx
 */
public class MqttBroker {
    private static int packetId = 0;
    public static void main(String[] args) {
        System.setProperty(ClientLogger.CLIENT_LOG_ADDITIVE, "true");
  /*      new Thread(new Runnable() {
=======
        // 端到云需要支持离线消息，云到端不需要支持离线消息？
        new Thread(new Runnable() {
>>>>>>> 79632f2b472a0b0a60436bf74f3cd823a751805f
            @Override
            public void run() {
                DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer-1");
                consumer.setNamesrvAddr("localhost:9876");
                consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
                try {
                    consumer.subscribe("push", "*");
                } catch (MQClientException e) {
                    e.printStackTrace();
                }
                consumer.registerMessageListener(new MessageListenerOrderly() {
                    @Override
                    public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                        context.setAutoCommit(true);
                        msgs.forEach(messageExt -> {
                            System.out.println(LocalDateTime.now() + " receive: " + new String(messageExt.getBody()));
                            System.out.println(messageExt.getUserProperty("MQTT_TOPIC"));
                            String topic = messageExt.getUserProperty("MQTT_TOPIC");
                            if (topic != null) {
                                Channel channel = MqttMessageHandler.SUBSCRIBE_MAP.get(topic);
                                if (channel != null) {
                                    if (channel.isActive()) {

                                        int remainingLength = 2 + topic.getBytes().length + 2 + messageExt.getBody().length;
                                        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, remainingLength);
                                        MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topic, packetId++);
                                        MqttPublishMessage mqttPublishMessage = new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, Unpooled.wrappedBuffer(messageExt.getBody()));

                                        channel.writeAndFlush(mqttPublishMessage).addListener(new ChannelFutureListener() {
                                            @Override
                                            public void operationComplete(ChannelFuture future) throws Exception {
                                                if (future.isSuccess()) {
                                                    System.out.println("发送消息成功！");
                                                } else {
                                                    System.err.println("发送消息失败！");
                                                }
                                            }
                                        });

                                    } else {
                                        // TODO 清理该 channel 所占用资源
                                        System.err.println("channel is not Active!");
                                    }
                                } else {
                                    System.err.println("还没有终端订阅该topic：" + topic);
                                }

                            }
                        });
                        return ConsumeOrderlyStatus.SUCCESS;
                    }
                });


                try {
                    consumer.start();
                } catch (MQClientException e) {
                    e.printStackTrace();
                }

                System.out.printf("Consumer Started.%n");
            }
        }).start();*/

        int port = 1883;
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new LoggingHandler(LogLevel.INFO));
                            pipeline.addLast(new MqttDecoder());
                            pipeline.addLast(MqttEncoder.INSTANCE);
                            pipeline.addLast(new MqttServerHandler());
                            pipeline.addLast(new MqttMessageHandler());
                        }
                    });
            ChannelFuture future = serverBootstrap.bind(port).sync();
            // TODO 这行可以不用？RocketMQ的NettyRemotingServer就没用
            future.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
