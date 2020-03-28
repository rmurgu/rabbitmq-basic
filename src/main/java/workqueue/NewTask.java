package workqueue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

/**
 * Publisher with a durable queue and persistent messages
 * Each task is delivered to exactly one worker
 * If one consumer will finish his task will take the next one from the queue
 */

public class NewTask {

    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

            String message = ".";
            int count = 0;

            while (count < 100) {
                String toSend = "Message " + count + ": " + message;

                channel.basicPublish("", TASK_QUEUE_NAME,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        toSend.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + toSend + "'");

                message = message + ".";
                count++;
                Thread.sleep(1000);
            }
        }
    }

}
