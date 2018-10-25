package com.amqp.pubsub;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;

/**
 * Producer class
 *
 * @author Administrator
 * @date
 */
public class Producer {

    @Test
    public void produce() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(AMQP.PROTOCOL.PORT);
        factory.setUsername("guest");
        factory.setPassword("guest");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        //在生产者，不需要定义queue，也不需要定义exchange，只需要要exchange的名字
        //queue和exchange只需要在客户端定义
        String exchangeName = "hi.fanout.exchange";

        for (int i = 0; i < 10; i++) {
            String message = "Hello RabbitMQ " + i;
            //广播类型不需要routingKey，但是不能写成null，可以写成空字符串""
            channel.basicPublish(exchangeName, "", null, message.getBytes("UTF-8"));
        }

        channel.close();
        connection.close();
    }
}
