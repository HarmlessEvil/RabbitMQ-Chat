package ru.itmo.softwaredesign;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;

public class ReceiveLogs {
    private static final String EXCHANGE_NAME = "router";
    private static final String EXCHANGE_NAME_2 = "logs_3";
    private static final String EXCHANGE_NAME_3 = "logs_3_out";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(32769);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "");

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String[] message = new String(delivery.getBody(), StandardCharsets.UTF_8).split(":");

            if (message.length < 2) {
                return;
            }

            String username = message[0];
            String channelName = message[1];

            System.out.println(username + " joined channel " + channelName);

            /* Queue for incoming messages */
            channel.exchangeDeclare(EXCHANGE_NAME_2, "direct");
            String anotherQueueName = channel.queueDeclare().getQueue();
            channel.queueBind(anotherQueueName, EXCHANGE_NAME_2, channelName);

            /* Queue for broadcasting channel */
            Channel outChannel = connection.createChannel();
            outChannel.exchangeDeclare(EXCHANGE_NAME_3, "direct");
            String outQueueName = channel.queueDeclare().getQueue();
            outChannel.queueBind(outQueueName, EXCHANGE_NAME_3, channelName);

            DeliverCallback anotherDeliverCallback = (innerConsumerTag, innerDelivery) -> {
                String[] messageParts = new String(innerDelivery.getBody(), StandardCharsets.UTF_8).split(":");

                if (messageParts.length < 2 || !messageParts[0].equals(username)) {
                    return;
                }

                String innerMessage = messageParts[1];
                String formattedMessage = '[' + username + "] " + innerMessage;

                System.out.println(
                        " [x] Received " + formattedMessage + " (" + innerDelivery.getEnvelope().getRoutingKey() + ')'
                );

                outChannel.basicPublish(
                        EXCHANGE_NAME_3,
                        channelName,
                        null,
                        formattedMessage.getBytes(StandardCharsets.UTF_8)
                );
            };
            channel.basicConsume(anotherQueueName, true, anotherDeliverCallback, innerConsumerTag -> {
            });

            String response = "Welcome to " + channelName + ", " + username + '!';
            channel.basicPublish(
                    EXCHANGE_NAME,
                    delivery.getProperties().getReplyTo(),
                    null,
                    response.getBytes(StandardCharsets.UTF_8)
            );
        };

        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });
    }
}
