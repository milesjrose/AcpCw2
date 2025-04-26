package uk.ac.ed.acp.cw2.service;

import java.util.*;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
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


@Service
public class KafkaService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaService.class);
    private final String uid = "s2093547";
    private final ObjectMapper objectMapper = new ObjectMapper();
    private KafkaConsumer<String, String> consumer;
    private KafkaProducer<String, String> producer;

    public KafkaService(RuntimeEnvironment environment) {
        long startTime = System.currentTimeMillis();
        logger.info("KafkaService constructor took {} ms", System.currentTimeMillis() - startTime);
        this.producer = new KafkaProducer<>(getKafkaProperties(environment));
        this.consumer = new KafkaConsumer<>(getKafkaProperties(environment));
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

    public List<String> receive(String readTopic, int timeoutInMsec, int messageCount, List<String> requiredFields) {
        // Check inputs
        boolean checkCount = (messageCount != 0);
        boolean checkTime = (timeoutInMsec != 0);
        boolean ignoreFields = (requiredFields == null);
        String prefix = !ignoreFields ? "[With Validation]" : "";
        Integer runType = (checkCount ? 1 : 0) + (checkTime ? 2 : 0); // 0 = neither, 1 = count, 2 = time, 3 = both
        switch (runType){
            case 0: logger.error(prefix + "Requesting read with no message count or timeout"); return null;
            case 1: logger.info(prefix + "Reading topic {}: count={}", readTopic, messageCount); break;
            case 2: logger.info(prefix + "Reading topic {}: timeOut={}", readTopic, timeoutInMsec); break;
            case 3: logger.info(prefix + "Reading topic {}: timeOut={}, count={}", readTopic, timeoutInMsec, messageCount); break;
        }
        // Setup
        List<String> messages = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        // Subscribe to topic
        try {
            consumer.unsubscribe();
        } catch (Exception e) {
            logger.error("Error unsubscribing from topics - may cause incorrect offset", e.getMessage());
        } finally {
            try {
                consumer.subscribe(Collections.singletonList(readTopic));
            } catch (Exception e) {
                logger.error("Error subscribing to topic: {}", e.getMessage());
                return null;
            }
        }
        // Receive messages
        while (checkCount || 
                (checkTime && (System.currentTimeMillis() - startTime < timeoutInMsec))) {
            try{
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<String, String> record : records) {
                    String message = record.value();
                    if (ignoreFields || Parser.isValidMessage(message, requiredFields)) {
                        messages.add(message);
                        if (checkCount && checkCount(messages.size(), messageCount)){
                            checkCount = false; break;
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Error polling messages from Kafka topic", e);
            } 
        }
        // Output
        switch (runType){
            case 1: logger.info(prefix + "Received {}/{} messages in {}ms", messages.size(), messageCount, System.currentTimeMillis() - startTime); break;
            case 2: logger.info(prefix + "Received {} messages in {}/{}ms", messages.size(), System.currentTimeMillis() - startTime, timeoutInMsec); break;
            case 3: logger.info(prefix + "Received {}/{} messages in {}/{}ms", messages.size(), messageCount, System.currentTimeMillis() - startTime, timeoutInMsec); break;
        }
        // Return received messages
        return messages;
    }

    // Public methods
    public List<String> receiveTimeout(String readTopic, int timeoutInMsec){return receive(readTopic, timeoutInMsec, 0, null);}
    public List<String> receiveTimeout(String readTopic, int timeoutInMsec, List<String> requiredFields){return receive(readTopic, timeoutInMsec, 0, requiredFields); }
    public List<String> receiveCount(String readTopic, int messageCount){return receive(readTopic, 0, messageCount, null);}
    public List<String> receiveCount(String readTopic, int messageCount, List<String> requiredFields){return receive(readTopic, 0, messageCount, requiredFields);}

    private boolean checkCount(int count, int target){
        return (count >= target);
    }
    // ================================ Send ================================

    public boolean push(String topic, List<String> messages){
        logger.info("Pushing {} messages to {}", messages.size(), topic);
        for (int i = 0; i < messages.size(); i++){
            int count = 0;
            while (!send(topic, messages.get(i))){
                count++;
                logger.error("Error pushing to kafka ({}/{}). Retrying...", i, messages.size());
                if (count > 5){
                    logger.error("Aborting push after 5 retries. {}/{} messages pushed", i, messages.size());
                    return false;
                }
            }
        }
        return true;
    }

    public boolean push(String topic, String message){
        logger.info("Pushing to {}. Message:", message, topic);
        Integer count = 0;
        while (!send(topic, message)){
            count++;
            logger.error("Error pushing to kafka. Retrying...");
            if (count > 5){
                logger.error("Aborting push after 5 retries.");
                return false;
            }
        }
        return true;
    }

    // Sends a message to a topic
    private boolean send(String topic, String message){
        try{
            producer.send(new ProducerRecord<>(topic, message), (recordMetadata, ex) -> {
                if (ex != null) ex.printStackTrace();
                else logger.debug("Pushed to {}: message={}", topic, message);
            }).get(5000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public boolean pushCountMessages(String writeTopic, int messageCount){
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
        push(writeTopic, messages);
        return true;
    }
}
