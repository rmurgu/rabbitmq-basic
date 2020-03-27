package helloworld;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Publisher {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(QUEUE_NAME, false, false, false, null);


            String message = ".";
            int count = 0;

            while (count <= 100) {
                String toSend = "Message " + count + ": " + message;
                channel.basicPublish("", QUEUE_NAME, null, toSend.getBytes());
                System.out.println(" [x] Sent '" + toSend + "'");
                message += ".";
                count++;
                Thread.sleep(1000);
            }

        }

    }

}
