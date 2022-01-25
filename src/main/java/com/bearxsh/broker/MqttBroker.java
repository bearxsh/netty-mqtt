package com.bearxsh.broker;

import com.bearxsh.broker.handler.MqttMessageHandler;
import com.bearxsh.broker.handler.MqttServerHandler;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

/**
 * @author bearx
 */
public class MqttBroker {
    private static int packetId = 0;
    public static void main(String[] args) {
        new Thread(new Runnable() {
            @Override
            public void run() {

                /*
                 * Instantiate with specified consumer group name.
                 */
                DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name_4");

                consumer.setNamesrvAddr("localhost:9876");
                /*
                 * Specify where to start in case the specific consumer group is a brand-new one.
                 */
                consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

                /*
                 * Subscribe one more topic to consume.
                 */
                try {
                    consumer.subscribe("push", "*");
                } catch (MQClientException e) {
                    e.printStackTrace();
                }

                /*
                 *  Register callback to execute on arrival of messages fetched from brokers.
                 */
                consumer.registerMessageListener(new MessageListenerConcurrently() {

                    @Override
                    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                                    ConsumeConcurrentlyContext context) {
                        System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                        msgs.forEach(messageExt -> {
                            System.out.println(LocalDateTime.now() + " receive: " + new String(messageExt.getBody()));
                            System.out.println(messageExt.getUserProperty("MQTT_TOPIC"));
                            String topic = messageExt.getUserProperty("MQTT_TOPIC");
                            if (topic != null) {
                                Set<Channel> channelSet = MqttMessageHandler.SUBSCRIBE_MAP.get(topic);
                                channelSet.forEach(channel -> {
                                    if (channel.isActive()) {
                                        int remainingLength = 2 + topic.getBytes().length + 2 + messageExt.getBody().length;
                                        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_LEAST_ONCE, false, remainingLength);
                                        MqttPublishVariableHeader mqttPublishVariableHeader = new MqttPublishVariableHeader(topic, packetId++);
                                        MqttPublishMessage mqttPublishMessage = new MqttPublishMessage(mqttFixedHeader, mqttPublishVariableHeader, Unpooled.wrappedBuffer(messageExt.getBody()));
                                        channel.writeAndFlush(mqttPublishMessage);
                                    }
                                });
                            }
                        });
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                });

                try {
                    consumer.start();
                } catch (MQClientException e) {
                    e.printStackTrace();
                }

                System.out.printf("Consumer Started.%n");
            }
        }).start();

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
