package ru.itmo.softwaredesign;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.StringJoiner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class EmitLog {
    private static final String EXCHANGE_NAME = "router";
    private static final String EXCHANGE_NAME_2 = "logs_3";
    private static final String EXCHANGE_NAME_3 = "logs_3_out";

    public static void main(@NotNull String[] argv) throws Exception {
        final Scanner scanner = new Scanner(System.in);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(32769);
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            String username = argv.length < 1 ? "guest" : argv[0];
            String channelKey = argv.length < 2 ? "main" : argv[1];

            String packedMessage = new StringJoiner(":").add(username).add(channelKey).toString();

            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

            /* Subscribe to reply back */
            String callbackQueue = channel.queueDeclare().getQueue();
            channel.queueBind(callbackQueue, EXCHANGE_NAME, callbackQueue);
            final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

            channel.basicConsume(callbackQueue, true, (tag, delivery) -> {
                System.out.println("Welcome to " + channelKey + ", " + username + '!');

                channel.exchangeDeclare(EXCHANGE_NAME_2, "direct");

                /* Show all messages */
                final BlockingQueue<String> messages = new ArrayBlockingQueue<>(1);

                Channel outChannel = connection.createChannel();
                outChannel.exchangeDeclare(EXCHANGE_NAME_3, "direct");
                String messagesQueue = channel.queueDeclare().getQueue();
                outChannel.queueBind(messagesQueue, EXCHANGE_NAME_3, channelKey);
                outChannel.basicConsume(messagesQueue, true, (innerTag, innerDelivery) -> {
                    System.out.println(new String(innerDelivery.getBody(), StandardCharsets.UTF_8));
                }, consumerTag -> {
                });

                Runnable r = () -> {
                    while (true) {
                        try {
                            System.out.println(messages.take());
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                };
                Thread t = new Thread(r);
                t.start();

                while (true) {
                    String message = new StringJoiner(":").add(username).add(scanner.nextLine()).toString();

                    channel.basicPublish(
                            EXCHANGE_NAME_2,
                            channelKey,
                            null,
                            message.getBytes(StandardCharsets.UTF_8)
                    );
                }
            }, consumerTag -> {
                System.out.println("Cancel?");
            });

            /* Send welcoming message */
            channel.basicPublish(
                    EXCHANGE_NAME,
                    "",
                    new AMQP.BasicProperties.Builder().replyTo(callbackQueue).build(),
                    packedMessage.getBytes(StandardCharsets.UTF_8)
            );

            /* Blocking wait for answer */
            System.out.println(response.take());
        }
    }
}
