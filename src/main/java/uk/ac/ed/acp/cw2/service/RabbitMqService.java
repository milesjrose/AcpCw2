package uk.ac.ed.acp.cw2.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.rabbitmq.client.*;
import org.springframework.stereotype.Service;

import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;
import uk.ac.ed.acp.cw2.Utilities.Parser;

@Service
public class RabbitMqService {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqService.class);

    private ConnectionFactory factory = null;
    private Channel channel;

    private final String uid = "s2093547";
    private final ObjectMapper objectMapper = new ObjectMapper();

    public RabbitMqService(RuntimeEnvironment environment) {
        factory = new ConnectionFactory();
        factory.setHost(environment.getRabbitMqHost());
        factory.setPort(environment.getRabbitMqPort());
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(5000); // try every 5s
        try{
            Connection connection = factory.newConnection();
            channel = connection.createChannel();
        } catch (Exception e) {
            logger.error("Error initialising channel: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }
    
    // ================================ Push ================================

    public boolean push(String queueName, List<ObjectNode> messages) {
        try {
            channel.queueDeclare(queueName, false, false, false, null);
        } catch (Exception e) {
            logger.error("Error declaring queue: {}", e.getMessage());
            return false;
        }
        Integer sent = 0;
        Integer totalMessages = messages.size();
        for (ObjectNode message : messages) {
            try{
                String jsonMessage = objectMapper.writeValueAsString(message);
                Integer count = 0;
                while (!send(queueName, jsonMessage)){
                    count++;
                    logger.error("Error pushing message to queue. Retrying...");
                    if (count > 5){
                        logger.error("Aborting push after 5 retries. {}/{} messages pushed", sent, totalMessages);
                        return false;
                    }
                }
                sent++;
                // Reduce excessive logging
                if (totalMessages > 1){
                    logger.debug("Pushed message {}/{} to {}: {}", sent, totalMessages, queueName, jsonMessage);
                }
            } catch (Exception e) {
                logger.error("Error decoding json string {} - Skipping message", e.getMessage());
            }
        }
        if ((int)sent != (int)totalMessages){
            logger.error("Failed to push all messages to {} (pushed: {}/{})", queueName, sent, totalMessages);
            return false;
        }
        // Reduce excessive logging
        if (totalMessages > 1){
            logger.info("Sent {}/{} messages to {}", sent, totalMessages, queueName);
        }
        return true;
    }

    public boolean push(String queueName, ObjectNode message) {
        List<ObjectNode> messages = new ArrayList<>();
        messages.add(message);
        return push(queueName, messages);
    }

    private boolean send(String queueName, String message) {
        try{
            channel.basicPublish("", queueName, null, message.getBytes());
        } catch (Exception e) {logger.error("{}", e.getMessage());return false;}
        return true;
    }

    public boolean pushToQueue(String queueName, int messageCount) {
        List<ObjectNode> messages = new ArrayList<>();
        // Create messages
        for (int i = 0; i < messageCount; i++) {
            ObjectNode message = objectMapper.createObjectNode();
            message.put("uid", uid);
            message.put("counter", i);
            messages.add(message);
        }
        // Push messages
        return push(queueName, messages);
    }

    // ================================ Receive ================================

    private List<String> receive(String queueName, int timeoutInMsec, int messageCount, List<String> requiredFields){
        // Check inputs
        boolean checkCount = (messageCount != 0);
        boolean checkTime = (timeoutInMsec != 0);
        boolean ignoreFields = (requiredFields == null);
        Integer runType = (checkCount ? 1 : 0) + (checkTime ? 2 : 0); // 0 = neither, 1 = count, 2 = time, 3 = both
        String prefix = !ignoreFields ? "[With Validation]" : "";
        switch (runType){
            case 0: logger.error(prefix + "Requesting read with no message count or timeout"); return null;
            case 1: logger.info(prefix + "Reading queue {}: count={}", queueName, messageCount); break;
            case 2: logger.info(prefix + "Reading queue {}: timeOut={}", queueName, timeoutInMsec); break;
            case 3: logger.info(prefix + "Reading queue {}: timeOut={}, count={}", queueName, timeoutInMsec, messageCount); break;
        }
        // Setup
        List<String> messages = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(1);
        // Declare queue
        try {
            channel.queueDeclare(queueName, false, false, false, null);
        } catch (Exception e) {
            logger.error("Error declaring queue: {}", e.getMessage());
            return null;
        }
        // Define what to do when message is received
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            // If not ignoring fields, check if message is valid
            if (ignoreFields || Parser.isValidMessage(message, requiredFields)){
                messages.add(message);
                logger.debug("Received message {}/{}: {}", messages.size(), messageCount, message);
                if (checkCount && messages.size() >= messageCount){
                    try {channel.basicCancel(consumerTag);} catch (IOException e) {logger.error("Error cancelling consumer: {}", e.getMessage());}
                    latch.countDown();
                }
            }
        };
        // Receive messages
        try{
            String consumerTag = channel.basicConsume(queueName, true, deliverCallback, tag -> {});
            if (checkTime){
                // Wait for enough time, then cancel consumer
                Thread.sleep(startTime + timeoutInMsec - System.currentTimeMillis());
                channel.basicCancel(consumerTag);
            } else {
                // Wait for enough messages
                latch.await();
            }
        } catch (Exception e) {
            logger.error("Error receiving messages: {}", e.getMessage());
            return null;
        }
        // Output
        switch (runType){
            case 1: logger.info(prefix + "Received {}/{} messages in {}ms", messages.size(), messageCount, System.currentTimeMillis() - startTime); break;
            case 2: logger.info(prefix + "Received {} messages in {}/{}ms", messages.size(), System.currentTimeMillis() - startTime, timeoutInMsec); break;
            case 3: logger.info(prefix + "Received {}/{} messages in {}/{}ms", messages.size(), messageCount, System.currentTimeMillis() - startTime, timeoutInMsec); break;
        }
        // Return received messages
        logger.info("returning: {}", messages);
        return messages;
    }

    // Alt calls
    public List<String> receiveTimeout(String queueName, int timeoutInMsec) {return receive(queueName, timeoutInMsec, 0, null);}
    public List<String> receiveTimeout(String queueName, int timeoutInMsec, List<String> requiredFields) {return receive(queueName, timeoutInMsec, 0, requiredFields);}
    public List<String> receiveCount(String queueName, int messageCount) {return receive(queueName, 0, messageCount, null);}
    public List<String> receiveCount(String queueName, int messageCount, List<String> requiredFields) {return receive(queueName, 0, messageCount, requiredFields);}
    public List<String> receiveCountWithTimeout(String queueName, int timeoutInMsec, int messageCount) {return receive(queueName, timeoutInMsec, messageCount, null);}
    public List<String> receiveCountWithTimeout(String queueName, int timeoutInMsec, int messageCount, List<String> requiredFields) {return receive(queueName, timeoutInMsec, messageCount, requiredFields);}
    // ================================ Utility ================================
    public long getQueueMessageCount(String queueName) {
        // Declare queue
        try {
            channel.queueDeclare(queueName, false, false, false, null);
        } catch (Exception e) { 
            logger.error("Error declaring queue: {}", e.getMessage());
            return -1;
        }
        // Get message count
        try {return channel.messageCount(queueName);} 
        catch (IOException e) {
            logger.error("Error getting message count: {}", e.getMessage());
            return -1;
        }
    }
}

