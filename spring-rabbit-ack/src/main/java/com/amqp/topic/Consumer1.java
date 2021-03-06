package com.amqp.topic;

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
        factory.setHost("127.0.0.1");
        factory.setPort(AMQP.PROTOCOL.PORT);
        factory.setUsername("guest");
        factory.setPassword("guest");

        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        String EXCHANGE_NAME = "exchange.topic.x";
        String QUEUE_NAME = "queue.topic.q1";
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        String[] routingKeys = {"*.orange.*"};
        for (int i = 0; i < routingKeys.length; i++) {
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, routingKeys[i]);
        }

        Consumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [C] Received '" + message + "', 处理业务中...");
            }
        };

        channel.basicConsume(QUEUE_NAME, true, consumer);

        Thread.sleep(1000000);

        channel.close();
        connection.close();
    }
}
