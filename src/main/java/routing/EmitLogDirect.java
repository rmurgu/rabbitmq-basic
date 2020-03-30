package routing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * A simple logging system. Using a direct exchange and routing keys.
 *
 */

public class EmitLogDirect {

    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.exchangeDeclare(EXCHANGE_NAME, "direct");

            String message = "Log message number ";
            int count = 0;

            while (count < 100) {
                String toSend = message + count;

                if (count%2 == 0) {
                    channel.basicPublish(EXCHANGE_NAME, "orange", null, toSend.getBytes());
                    System.out.println(" [x] Sent ORANGE '" + toSend + "'");
                } else {
                    channel.basicPublish(EXCHANGE_NAME, "black", null, toSend.getBytes());
                    System.out.println(" [x] Sent BLACK '" + toSend + "'");
                }

                count++;
                Thread.sleep(1000);
            }
        }

    }

}
