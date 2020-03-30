package topics;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * A simple logging system. Using a topic exchange and composed routing key.
 *
 */

public class EmitLogTopic {

    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] args) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()){

            channel.exchangeDeclare(EXCHANGE_NAME, "topic");

            String message = "Log message number ";
            int count = 0;

            while (count < 100) {
                String toSend = message + count;
                String routingKey = "test.topic";

                channel.basicPublish(EXCHANGE_NAME, routingKey, null, toSend.getBytes());
                System.out.println(" [x] Sent on '" + routingKey + "' : '" + toSend + "'");

                count++;
                Thread.sleep(1000);
            }

        }

    }

}
