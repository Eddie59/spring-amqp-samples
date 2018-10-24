/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.samples.confirms;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SpringRabbitConfirmsReturnsApplication {

    private final CountDownLatch listenLatch = new CountDownLatch(1);
    private final CountDownLatch confirmLatch = new CountDownLatch(1);
    private final CountDownLatch returnLatch = new CountDownLatch(1);

    private static final String QUEUE = "spring.publisher.sample";

    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext context = SpringApplication.run(SpringRabbitConfirmsReturnsApplication.class,
                args);
        context.getBean(SpringRabbitConfirmsReturnsApplication.class).runDemo();
        context.close();
    }

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Bean
    public Queue queue() {
        return new Queue(QUEUE, false, false, true);
    }

/*
    @RabbitListener(queues = QUEUE)
	public void listen(String in) {
		System.out.println("Listener received: " + in);
		this.listenLatch.countDown();
	}
*/

    private void runDemo() throws Exception {
        setupCallbacks();

        this.rabbitTemplate.convertAndSend("",QUEUE,"abcdefg");
        //发送消息到默认的Exchange，消息的key为spring.publisher.sample
        // send()方法重载了一个CorrelationData对象的版本，当发布确认被启用时，这个对象将被传输到先前描述的回调函数中，这使得发送者对发送的消息进行确认。
        this.rabbitTemplate.convertAndSend("", QUEUE, "foo", new CorrelationData("消息1的相关性的内容"));

        if (this.confirmLatch.await(5, TimeUnit.SECONDS)) {
            System.out.println("确认到达Exchange");
        } else {
            System.out.println("没有确认到达Exchange");
        }

        if (this.listenLatch.await(5, TimeUnit.SECONDS)) {
            System.out.println("消费者已接收");
        } else {
            System.out.println("消费者没有接收");
        }

        //接收信息经过转换后的信息，信息还没有发送到Exchange之前，比如，把java对象转化为json以后，发送到Exchange之前，想知道转换后的结果
        this.rabbitTemplate.convertAndSend("", QUEUE + QUEUE, "bar", message -> {
            System.out.println("Message after conversion: " + message);
            return message;
        });

        if (this.returnLatch.await(5, TimeUnit.SECONDS)) {
            System.out.println("Return received");
        } else {
            System.out.println("Return NOT received");
        }
    }

    private void setupCallbacks() {

        //发送端成功发送到exchange，会回调这个方法
        this.rabbitTemplate.setConfirmCallback((correlation, ack, reason) -> {
            if (correlation != null) {
                System.out.println("Received " + (ack ? " ack " : " nack ") + "for correlation: " + correlation);
            }
            this.confirmLatch.countDown();
        });

        //发送的消息，要发送的Exchange是存在的，但是Exchange没有对应的queue，才会触发PublisherReturns
        this.rabbitTemplate.setReturnCallback((message, replyCode, replyText, exchange, routingKey) -> {
            System.out.println("Returned: " + message + "\nreplyCode: " + replyCode + "\nreplyText: " + replyText + "\nexchange/rk: " + exchange + "/" + routingKey);
            this.returnLatch.countDown();
        });

		/*
		 * Replace the correlation data with one containing the converted message in case
		 * we want to resend it after a nack.
		 * 使用另一个对象来替换correlationData对象，这个功能是什么？???不清楚这个机制有什么用？？？
		 */
        this.rabbitTemplate.setCorrelationDataPostProcessor((message, correlationData) -> {
            String correlationData_id = correlationData != null ? correlationData.getId() : null;
            System.out.println("correlationData_id:"+correlationData_id);
            return new CompleteMessageCorrelationData(correlationData_id, message);
        });


    }


    static class CompleteMessageCorrelationData extends CorrelationData {

        private final Message message;

        CompleteMessageCorrelationData(String id, Message message) {
            super(id);
            this.message = message;
        }

        public Message getMessage() {
            return this.message;
        }

        @Override
        public String toString() {
            return "CompleteMessageCorrelationData [id=" + getId() + ", message=" + this.message + "]";
        }

    }

}
