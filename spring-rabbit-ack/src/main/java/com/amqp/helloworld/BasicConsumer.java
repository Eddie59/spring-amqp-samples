package com.amqp.helloworld;

import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * BasicConsumer class
 *
 * @author Administrator
 * @date
 */
public class BasicConsumer {

    @Test
    public void consumerTest() throws Exception{
        ConnectionFactory connectionFactory=new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(AMQP.PROTOCOL.PORT);
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        Connection connection= connectionFactory.newConnection();
        Channel channel= connection.createChannel();

        String exchangeName="hello_Direct";
        String queueName="hello_queue";
        channel.exchangeDeclare(exchangeName,"direct");
        //关于队列声明queueDeclare的参数：第一个参数表示队列名称、
        // 第二个参数为是否持久化（true表示是，队列将在服务器重启时生存）、
        // 第三个参数为是否是独占队列（创建者可以使用的私有队列，断开后自动删除）、
        // 第四个参数为当所有消费者客户端连接断开时是否自动删除队列、
        // 第五个参数为队列的其他参数
        channel.queueDeclare(queueName,false,false,false,null);
        channel.queueBind(queueName,exchangeName,"hello");

        Consumer consumer=new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [C] Received '" + message + "'");
            }
        };

        //不是自动ack
//        channel.basicConsume(queueName,consumer);
        channel.basicConsume(queueName,true,consumer);

        Thread.sleep(5000);
        channel.close();
        connection.close();

    }

}
