package com.amqp.pubsub;

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
    public void consume() throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(AMQP.PROTOCOL.PORT);
        factory.setUsername("guest");
        factory.setPassword("guest");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        //每次接收一条信息
        channel.basicQos(1);

        String EXCHANGE_NAME = "hi.fanout.exchange";
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
        //暂时队列，消费者断开连接，队列删除
        String QUEUE_NAME = channel.queueDeclare().getQueue();
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "adfasd");

        Consumer consumer=new DefaultConsumer(channel){
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
