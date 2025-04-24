package uk.ac.ed.acp.cw2.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import uk.ac.ed.acp.cw2.Utilities.Parser;

@Service
public class RabbitMqService {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqService.class);

    private ConnectionFactory factory = null;

    private final String uid = "s2093547";
    private final ObjectMapper objectMapper = new ObjectMapper();

    public RabbitMqService(RuntimeEnvironment environment) {
        factory = new ConnectionFactory();
        factory.setHost(environment.getRabbitMqHost());
        factory.setPort(environment.getRabbitMqPort());
    }

    // ================================ Push ================================

    public void pushMessages(String queueName, List<ObjectNode> messages) {
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Declare queue
            channel.queueDeclare(queueName, false, false, false, null);

            // Push messages
            for (ObjectNode message : messages) {
                String jsonMessage = objectMapper.writeValueAsString(message);
                channel.basicPublish("", queueName, null, jsonMessage.getBytes());
                logger.debug("Pushed to {}: {}", queueName, jsonMessage);
            }

            // Return
            logger.debug("Sent {} messages to {}", messages.size(), queueName);
        } catch (Exception e) {
            logger.error("Error pushing messages to queue", e);
            throw new RuntimeException(e);
        }
    }

    public void pushMessage(String queueName, ObjectNode message) {
        List<ObjectNode> messages = new ArrayList<>();
        messages.add(message);
        pushMessages(queueName, messages);
    }

    public void pushToQueue(String queueName, int messageCount) {
        List<ObjectNode> messages = new ArrayList<>();
        // Create messages
        for (int i = 0; i < messageCount; i++) {
            ObjectNode message = objectMapper.createObjectNode();
            message.put("uid", uid);
            message.put("counter", i);
            messages.add(message);
        }
        // Push messages
        pushMessages(queueName, messages);
    }

    // ================================ Receive ================================

    public List<String> receiveFromQueue(String queueName, int timeoutInMsec, int messageCount){
        // Check inputs
        boolean ignoreCount = (messageCount == 0);
        boolean ignoreTime = (timeoutInMsec == 0);
        if (!ignoreCount && !ignoreTime){logger.info("Reading queue {}: timeOut={}, count={}", queueName, timeoutInMsec, messageCount);}
        else if (!ignoreCount){logger.info("Reading queue {}: timeOut={}", queueName, timeoutInMsec);}
        else if (!ignoreTime){logger.info("Reading queue {}: count={}", queueName, messageCount);}
        else {logger.error("Requesting read with no message count or timeout"); return new ArrayList<>();}
        // Setup
        List<String> messages = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(1);

        // Receive
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            // Declare queue
            channel.queueDeclare(queueName, false, false, false, null);
            // Define receiver
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                messages.add(message);
                logger.debug("Received: {}", message);
                
                if (!ignoreCount && messages.size() >= messageCount) {
                    try {
                        channel.basicCancel(consumerTag);
                    } catch (IOException e) {
                        logger.error("Error cancelling consumer: {}", e.getMessage());
                    }
                    latch.countDown();
                }
            };
            // Start receiving
            String consumerTag = channel.basicConsume(queueName, true, deliverCallback, tag -> {});
            long remainingTime = timeoutInMsec - (System.currentTimeMillis() - startTime);
            // Wait until either max messages received or timeout
            while ((!ignoreCount || messages.size() < messageCount) && 
                   (!ignoreTime || remainingTime > 0)) {
                long sleepTime = Math.min(100, remainingTime);
                Thread.sleep(sleepTime); // Small sleep to prevent busy waiting
                remainingTime = timeoutInMsec - (System.currentTimeMillis() - startTime);
            }
            // Cancel consumer if still active
            if (channel.isOpen()) {
                channel.basicCancel(consumerTag);
            }

            if (!ignoreCount && !ignoreTime){logger.info("Received {}/{} messages in {}ms/{}ms (timeout={}ms)", messages.size(), messageCount, System.currentTimeMillis() - startTime, timeoutInMsec+ 200, timeoutInMsec);}
            else if (ignoreCount){logger.info("Received {} messages in {}ms/{}ms (timeout={}ms)", messages.size(), System.currentTimeMillis() - startTime, timeoutInMsec+ 200, timeoutInMsec);}
            else {logger.info("Received {}/{} messages in {}ms", messages.size(), messageCount, System.currentTimeMillis() - startTime);}
            return messages;
        } catch (Exception e) {
            logger.error("Error receiving messages from queue", e);
            throw new RuntimeException(e);
        }
    }

    public List<String> receiveFromQueueTimeout(String queueName, int timeoutInMsec) {
        return receiveFromQueue(queueName, timeoutInMsec, 0);
    }

    public List<String> receiveFromQueueCount(String queueName, int messageCount) {
        return receiveFromQueue(queueName, 0, messageCount);
    }

    public List<String> receiveValidMessagesFromQueue(String queueName, int messageCount, List<String> requiredFields) {
        logger.info("Receiving {} valid messages from queue: {}", messageCount, queueName);
        long startTime = System.currentTimeMillis();
        List<String> messages = new ArrayList<>();
        AtomicInteger receivedCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // Declare queue
            channel.queueDeclare(queueName, false, false, false, null);

            // Define receiver
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                if (Parser.isValidMessage(message, requiredFields)) {
                    messages.add(message);
                    receivedCount.incrementAndGet();
                    logger.debug("Received valid message: {}", message);
                    if (receivedCount.get() >= messageCount) {
                        try {
                            channel.basicCancel(consumerTag);
                            latch.countDown();
                        } catch (IOException e) {
                            logger.error("Error cancelling consumer: {}", e.getMessage());
                        }
                    }
                }
            };

            // Start receiving messages
            channel.basicConsume(queueName, true, deliverCallback, tag -> {
                latch.countDown();
            });

            // Wait for the consumer to be cancelled (Message count hit)
            latch.await();

            logger.info("Received {}/{} valid messages in {}ms, queue={}", messages.size(), messageCount, System.currentTimeMillis() - startTime, queueName);
            return messages;
        } catch (Exception e) {
            logger.error("Error receiving messages from queue: {}", e.getMessage());
            return messages;
        }
    }
}

