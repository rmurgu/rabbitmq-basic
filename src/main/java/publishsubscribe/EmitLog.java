package publishsubscribe;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * A simple logging system. Using a fanout exchange.
 * It will consist of two programs.
 * The first will emit log messages and the second will receive and print them.
 * Every running copy of the receiver program will get the messages.
 */

public class EmitLog {

    private static final String EXCHANGE_NAME = "logs";

    public static void main(String[] args) throws Exception{

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");

            String message = "Log message number ";
            int count = 0;

            while (count < 100) {
                String toSend = message + count;
                channel.basicPublish(EXCHANGE_NAME, "", null, toSend.getBytes());
                System.out.println(" [x] Sent '" + toSend + "'");
                count++;
                Thread.sleep(1000);
            }

        }

    }

}
