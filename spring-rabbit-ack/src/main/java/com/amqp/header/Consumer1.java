package com.amqp.header;

import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
        Channel channel = connection.createChannel();

        String EXCHANGE_NAME = "exchange.hearders";
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.HEADERS);

        String queueName = channel.queueDeclare().getQueue();
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("x-match", "all");
        arguments.put("api", "login");
        arguments.put("version", 1.0);
//        arguments.put("dataType", "json");


        //队列绑定时需要指定参数,注意虽然不需要路由键但仍旧不能写成null，需要写成空字符串""
        channel.queueBind(queueName, EXCHANGE_NAME, "", arguments);
        Consumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(message);
            }
        };

        channel.basicConsume(queueName, true, consumer);
        Thread.sleep(100000);
    }
}
