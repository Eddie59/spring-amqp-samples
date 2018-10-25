package com.amqp.header;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Producer class
 *
 * @author Administrator
 * @date
 */
public class Producer {

    @Test
    public void testBasicPublish() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(AMQP.PROTOCOL.PORT);
        factory.setUsername("guest");
        factory.setPassword("guest");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        //发送消息的properties
        Map<String, Object> heardersMap = new HashMap<>();
        heardersMap.put("api", "login");
        heardersMap.put("version", 1.0);
//        heardersMap.put("radom", UUID.randomUUID().toString());
        AMQP.BasicProperties.Builder properties = new AMQP.BasicProperties().builder().headers(heardersMap);

        channel.basicPublish("exchange.hearders", "", properties.build(), "Hello RabbitMQ!".getBytes("UTF-8"));

        channel.close();
        connection.close();
    }

}
