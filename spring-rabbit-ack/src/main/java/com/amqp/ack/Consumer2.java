package com.amqp.ack;

import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Consumer1 class
 *
 * @author Administrator
 * @date
 */
public class Consumer2 {
    @Test
    public void testBasicConsumer1() throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(AMQP.PROTOCOL.PORT);
        factory.setUsername("guest");
        factory.setPassword("guest");

        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();
        String EXCHANGE_NAME = "exchange.direct";
        String QUEUE_NAME = "queue_name";
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "key");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(message+" envelope.getDeliveryTag():"+envelope.getDeliveryTag());

                if (message.contains(":0")){
                    //客户端拒绝消息，false告诉服务器可以删除此删除，true表示消息重新入队，不删除，下次再发
                    channel.basicReject(envelope.getDeliveryTag(),false);
                }
                else if(message.contains(":1")){
                    //客户端拒绝消息，true表示消息重新入队，不删除，下次再发
                    //服务器发meaage 1被拒绝，消息重新入队，下次发message 2 下次发message 3、4，发完后再发message1，又重新入队，又发message1,死循环
                    channel.basicReject(envelope.getDeliveryTag(),true);

                }
                else {
                    //服务器会把所有消息推送过来Ready变为了，Unacked为消息数，表示服务器全部推过去了，但客户端都没有回复
                    // 执行channel.basicAck（false）客户端给服务器应答，一条消息已收到，Unacked会减1
                    //如果为true，表示给服务器应答“所有消息已确认收到”，Unacked直接变0
                    channel.basicAck(envelope.getDeliveryTag(), false);//false 是否多条确认
                }
            }
        };

        boolean autoAck=false;
        channel.basicConsume(QUEUE_NAME, autoAck, consumer);

        Thread.sleep(100000);
    }
}

