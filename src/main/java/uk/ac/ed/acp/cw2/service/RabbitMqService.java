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
import java.util.Random;

@Service
public class RabbitMqService {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqService.class);
    private final RuntimeEnvironment environment;
    private final String[] stockSymbols = "AAPL,MSFT,GOOG,AMZN,TSLA,JPMC,CATP,UNIL,LLOY".split(",");

    private ConnectionFactory factory = null;

    private final String uid = "s2093547";
    private final ObjectMapper objectMapper = new ObjectMapper();

    public RabbitMqService(RuntimeEnvironment environment) {
        this.environment = environment;
        factory = new ConnectionFactory();
        factory.setHost(environment.getRabbitMqHost());
        factory.setPort(environment.getRabbitMqPort());
    }

    public void sendStockSymbols(String queueName, int symbolCount) {
        logger.info("=-= Writing {} symbols in queue {}", symbolCount, queueName);

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(queueName, false, false, false, null);

            for (int i = 0; i < symbolCount; i++) {
                final String symbol = stockSymbols[new Random().nextInt(stockSymbols.length)];
                final String value = String.valueOf(i);

                String message = String.format("%s:%s", symbol, value);

                channel.basicPublish("", queueName, null, message.getBytes());
                System.out.println(" [x] Sent message: " + message + " to queue: " + queueName);
            }

            logger.info("{} record(s) sent to Kafka\n", symbolCount);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> receiveStockSymbols(String queueName, int consumeTimeMsec) {
        logger.info(String.format("=-= Reading stock-symbols from queue %s", queueName));
        List<String> result = new ArrayList<>();

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.printf("[%s]:%s -> %s", queueName, delivery.getEnvelope().getRoutingKey(), message);
                result.add(message);
            };

            System.out.println("start consuming events - to stop press CTRL+C");
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
            Thread.sleep(consumeTimeMsec);

            System.out.printf("done consuming events. %d record(s) received\n", result.size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    public void pushToQueue(String queueName, int messageCount) {
        logger.info("=-= Pushing {} messages to queue {}", messageCount, queueName);
        List<ObjectNode> messages = new ArrayList<>();

        for (int i = 0; i < messageCount; i++) {
            ObjectNode message = objectMapper.createObjectNode();
            message.put("uid", uid);
            message.put("counter", i);
            messages.add(message);
        }
        pushMessages(queueName, messages);
    }

    public List<String> receiveFromQueue(String queueName, int timeoutInMsec) {
        logger.info("=-= Reading messages from queue {} with timeout {} msec", queueName, timeoutInMsec);
        List<String> messages = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        long maxExecutionTime = timeoutInMsec + 200;

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                messages.add(message);
                logger.debug("Received message: {}", message);
            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});

            while (System.currentTimeMillis() - startTime < maxExecutionTime) {
                if (System.currentTimeMillis() - startTime >= timeoutInMsec) {
                    break;
                }
                Thread.sleep(10);
            }
            logger.info("Returned in {}, timeout was {} (max:{})", System.currentTimeMillis() - startTime, timeoutInMsec, timeoutInMsec + 200);
            return messages;
        } catch (Exception e) {
            logger.error("Error receiving messages from queue", e);
            throw new RuntimeException(e);
        }
    }

    public void pushMessages(String queueName, List<ObjectNode> messages) {
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(queueName, false, false, false, null);

            for (ObjectNode message : messages) {
                String jsonMessage = objectMapper.writeValueAsString(message);
                channel.basicPublish("", queueName, null, jsonMessage.getBytes());
                logger.info("Sent message {} to queue {}", jsonMessage, queueName);
            }

        } catch (Exception e) {
            logger.error("Error pushing messages to queue", e);
            throw new RuntimeException(e);
        }
    }

    public void pushMessage(String queueName, ObjectNode message) {
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(queueName, false, false, false, null);
            String jsonMessage = objectMapper.writeValueAsString(message);
            channel.basicPublish("", queueName, null, jsonMessage.getBytes());
            logger.info("Sent message {} to queue {}", jsonMessage, queueName);
        } catch (Exception e) {
            logger.error("Error pushing message to queue", e);
            throw new RuntimeException(e);
        }
    }
}
