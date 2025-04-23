package uk.ac.ed.acp.cw2.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;
import uk.ac.ed.acp.cw2.Utilities.Parser;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class KafkaService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaService.class);
    private final RuntimeEnvironment environment;
    private final String uid = "s2093547";
    private final ObjectMapper objectMapper = new ObjectMapper();
    public KafkaService(RuntimeEnvironment environment) {
        this.environment = environment;
    }


    /**
     * Constructs Kafka properties required for KafkaProducer and KafkaConsumer configuration.
     *
     * @param environment the runtime environment providing dynamic configuration details
     *                     such as Kafka bootstrap servers.
     * @return a Properties object containing configuration properties for Kafka operations.
     */
    private Properties getKafkaProperties(RuntimeEnvironment environment) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", environment.getKafkaBootstrapServers());
        kafkaProps.put("acks", "all");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.setProperty("enable.auto.commit", "true");
        kafkaProps.put("acks", "all");
        kafkaProps.put("group.id", UUID.randomUUID().toString());
        kafkaProps.setProperty("auto.offset.reset", "earliest");
        kafkaProps.setProperty("enable.auto.commit", "true");

        if (environment.getKafkaSecurityProtocol() != null) {
            kafkaProps.put("security.protocol", environment.getKafkaSecurityProtocol());
        }
        if (environment.getKafkaSaslMechanism() != null) {
            kafkaProps.put("sasl.mechanism", environment.getKafkaSaslMechanism());
        }
        if (environment.getKafkaSaslJaasConfig() != null) {
            kafkaProps.put("sasl.jaas.config", environment.getKafkaSaslJaasConfig());
        }
        return kafkaProps;
    }

    // ================================ Receive ================================

    public List<String> receiveFromTopic(String readTopic, int timeoutInMsec, int count) {
        // Check inputs
        boolean ignoreCount = (count == 0);
        boolean ignoreTime = (timeoutInMsec == 0);
        if (!ignoreCount && !ignoreTime) {
            logger.info("Reading topic {}: timeOut={}, count={}", readTopic, timeoutInMsec, count);
        } else if (!ignoreCount) {
            logger.info("Reading topic {}: timeOut={}", readTopic, timeoutInMsec);
        } else if (!ignoreTime) {
            logger.info("Reading topic {}: count={}", readTopic, count);
        } else {
            logger.error("Requesting read with no message count or timeout");
            return new ArrayList<>();
        }

        // Setup
        Properties kafkaProps = getKafkaProperties(environment);
        List<String> messages = Collections.synchronizedList(new ArrayList<>());
        long startTime = System.currentTimeMillis();
        long maxExecutionTime = timeoutInMsec + 200;
        CountDownLatch latch = new CountDownLatch(1);
        
        // Receive
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps)) {
            consumer.subscribe(Collections.singletonList(readTopic));

            // Start a separate thread for polling
            Thread consumerThread = new Thread(() -> {
                try {
                    while ((ignoreTime || System.currentTimeMillis() - startTime < maxExecutionTime) && 
                           (ignoreCount || messages.size() < count)) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
                        for (ConsumerRecord<String, String> record : records) {
                            messages.add(record.value());
                            logger.debug("Received message: {}", record.value());
                            if (!ignoreCount && messages.size() >= count) {
                                latch.countDown();
                                break;
                            }
                        }
                        if (!ignoreTime && System.currentTimeMillis() - startTime >= maxExecutionTime) {
                            latch.countDown();
                            break;
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error in consumer thread: {}", e.getMessage());
                    latch.countDown();
                }
            });

            consumerThread.start();
            latch.await();
            consumerThread.join();

            // Log results
            if (!ignoreCount && !ignoreTime) {
                logger.info("Received {}/{} messages in {}/{}ms (timeout={}ms)", 
                           messages.size(), count, 
                           System.currentTimeMillis() - startTime, 
                           timeoutInMsec + 200, timeoutInMsec);
            } else if (!ignoreCount) {
                logger.info("Received {} messages in {}/{}ms (timeout={}ms)", 
                           messages.size(), 
                           System.currentTimeMillis() - startTime, 
                           timeoutInMsec + 200, timeoutInMsec);
            } else {
                logger.info("Received {}/{} messages in {}ms", 
                           messages.size(), count, 
                           System.currentTimeMillis() - startTime);
            }
            return messages;
        } catch (Exception e) {
            logger.error("Error receiving messages from Kafka topic", e);
            return messages;
        }
    }

    public List<String> receiveFromTopic(String readTopic, int timeoutInMsec){
        return receiveFromTopic(readTopic, timeoutInMsec, 0);
    }

    public List<String> receiveFromTopicCount(String readTopic, int messageCount){
        return receiveFromTopic(readTopic, 0, messageCount);
    }


    public List<String> receiveValidMessagesFromTopic(String readTopic, int messageCount, List<String> requiredFields) {
        long startTime = System.currentTimeMillis();
        List<String> messages = new ArrayList<>();
        AtomicInteger receivedCount = new AtomicInteger(0);
        Properties kafkaProps = getKafkaProperties(environment);
        
        // Receive messages from topic, check if they are valid and add to list
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps)) {
            consumer.subscribe(Collections.singletonList(readTopic));

            while (receivedCount.get() < messageCount) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                
                for (ConsumerRecord<String, String> record : records) {
                    if (Parser.isValidMessage(record.value(), requiredFields)) {
                        messages.add(record.value());
                        if (receivedCount.incrementAndGet() >= messageCount) {break;}
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error receiving messages from topic: {}", e.getMessage());
        }
        logger.info("Returned {}/{} messages in {} ms)", messages.size(), messageCount, System.currentTimeMillis() - startTime);
        return messages;
    }

    // ================================ Send ================================

    public void send(String topic, List<String> messages){
        logger.info("Pushing {} messages to {}", messages.size(), topic);
        Properties kafkaProps = getKafkaProperties(environment);
        try(var producer = new KafkaProducer<String, String>(kafkaProps)) {
            for (String message : messages){
                // Send message, if no response after 5 seconds, throw exception
                producer.send(new ProducerRecord<>(topic, message), (recordMetadata, ex) -> {
                    if (ex != null)
                        ex.printStackTrace();
                else
                    logger.debug("Pushed to {}: message={}", topic, message);
                }).get(5000, TimeUnit.MILLISECONDS);
            }
        } catch (ExecutionException e) {
            logger.error("execution exc: " + e);
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            logger.error("timeout exc: " + e);
        } catch (InterruptedException e) {
            logger.error("interrupted exc: " + e);
            throw new RuntimeException(e);
        }
    }

    public void send(String topic, String message){
        List<String> messages = new ArrayList<>();
        messages.add(message);
        send(topic, messages);
    }

    public boolean pushToTopic(String writeTopic, int messageCount){
        List<String> messages = new ArrayList<>();

        for (int i = 0; i < messageCount; i++) {
            try{
            ObjectNode message = objectMapper.createObjectNode();
            message.put("uid", uid);
            message.put("counter", i);

            String jsonMessage = objectMapper.writeValueAsString(message);
            messages.add(jsonMessage);
            }
            catch (Exception e){
                logger.error("Error creating message {}", i, e);
            }
        }
        send(writeTopic, messages);
        return true;
    }
}
