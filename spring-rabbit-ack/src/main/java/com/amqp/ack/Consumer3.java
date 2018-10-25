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
public class Consumer3 {
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
                if (message.contains(":2")){
                    //basicRecover重新投递，一开始服务器把所有消息推过来，消费者去确认，到了消息2，消费者把没有确认的消息重新投递，
                    //也就是把消息2、3、4重新投递
                    //true表示重新投递的消息是否允许此消费者消费，false表示不允许此消费者消费
                    //不太懂basicRecover？？？
                    channel.basicRecover(true);
                }
                else {
                    channel.basicAck(envelope.getDeliveryTag(),false);
                }

            }
        };

        boolean autoAck=false;
        channel.basicConsume(QUEUE_NAME, autoAck, consumer);

        Thread.sleep(100000);
    }
}

