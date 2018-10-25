package com.amqp.helloworld;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;

/**
 * BasicPublish class
 *
 * @author Administrator
 * @date
 */
public class BasicPublish {

    @Test
    public void publishTest() throws Exception{
        ConnectionFactory connectionFactory=new ConnectionFactory();

        //连接        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(AMQP.PROTOCOL.PORT);
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
        Connection connection= connectionFactory.newConnection();
        //频道
        Channel channel= connection.createChannel();
        //声明
        String exchangeName="hello_Direct";
        String queueName="hello_queue";
        channel.exchangeDeclare(exchangeName,"direct");
        channel.queueDeclare(queueName,false,false,false,null);
        channel.queueBind(queueName,exchangeName,"hello");
        //发送
        channel.basicPublish(exchangeName,"hello",null,"Hello RabbitMQ".getBytes("UTF-8"));

        channel.close();
        connection.close();
    }
}
