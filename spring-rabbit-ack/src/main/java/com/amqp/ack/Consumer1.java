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
public class Consumer1 {
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

        //消费者最多一次接受4条消息，如果不设置，默认接受队列所有的消息
        channel.basicQos(4);

//        GetResponse response = channel.basicGet(QUEUE_NAME, false);
//        byte[] body = response.getBody();
//        System.out.println(new String(body).toString());

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(message);
            }
        };

        channel.basicConsume(QUEUE_NAME, true, consumer);

        Thread.sleep(100000);
    }
}

