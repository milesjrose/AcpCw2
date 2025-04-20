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
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import uk.ac.ed.acp.cw2.controller.KafkaController;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;
import uk.ac.ed.acp.cw2.model.Message;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class KafkaService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);
    private final RuntimeEnvironment environment;
    private final String[] stockSymbols = "AAPL,MSFT,GOOG,AMZN,TSLA,JPMC,CATP,UNIL,LLOY".split(",");
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

    public void sendStockSymbols(String symbolTopic, int symbolCount) {
        logger.info(String.format("Writing %d symbols in topic %s", symbolCount, symbolTopic));
        Properties kafkaProps = getKafkaProperties(environment);

        try (var producer = new KafkaProducer<String, String>(kafkaProps)) {
            for (int i = 0; i < symbolCount; i++) {
                final String key = stockSymbols[new Random().nextInt(stockSymbols.length)];
                final String value = String.valueOf(i);

                producer.send(new ProducerRecord<>(symbolTopic, key, value), (recordMetadata, ex) -> {
                    if (ex != null)
                        ex.printStackTrace();
                    else
                        logger.info(String.format("Produced event to topic %s: key = %-10s value = %s%n", symbolTopic, key, value));
                }).get(1000, TimeUnit.MILLISECONDS);
            }
            logger.info(String.format("%d record(s) sent to Kafka\n", symbolCount));
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

    public List<AbstractMap.SimpleEntry<String, String>> receiveStockSymbols(String symbolTopic, int consumeTimeMsec) {
        logger.info(String.format("Reading stock-symbols from topic %s", symbolTopic));
        Properties kafkaProps = getKafkaProperties(environment);

        var result = new ArrayList<AbstractMap.SimpleEntry<String, String>>();

        try (var consumer = new KafkaConsumer<String, String>(kafkaProps)) {
            consumer.subscribe(Collections.singletonList(symbolTopic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(consumeTimeMsec));
            for (ConsumerRecord<String, String> record : records) {
                logger.info(String.format("[%s] %s: %s %s %s %s", record.topic(), record.key(), record.value(), record.partition(), record.offset(), record.timestamp()));
                result.add(new AbstractMap.SimpleEntry<>(record.key(), record.value()));
            }
        }

        return result;
    }

    public List<String> receiveFromTopic(String readTopic, int timeoutInMsec) {
        logger.info("Reading messages from topic {} with timeout {} msec", readTopic, timeoutInMsec);
        Properties kafkaProps = getKafkaProperties(environment);
        List<String> messages = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        long maxExecutionTime = timeoutInMsec + 200;

        try (var consumer = new KafkaConsumer<String, String>(kafkaProps)) {
            consumer.subscribe(Collections.singletonList(readTopic));

            while (System.currentTimeMillis() - startTime < maxExecutionTime) {
                if (System.currentTimeMillis() - startTime >= timeoutInMsec) {
                    break;
                }

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<String, String> record : records) {
                    messages.add(record.value());
                    logger.debug("Received message: {}", record.value());
                }
            }

            logger.info("Returned in {}, timeout was {} (max:{})",
                    System.currentTimeMillis() - startTime, timeoutInMsec, timeoutInMsec + 200);
            return messages;
        } catch (Exception e) {
            logger.error("Error receiving messages from Kafka topic", e);
            return null;
        }
    }

    public boolean pushToTopic(String writeTopic, int messageCount){
        logger.info("Pushing {} messages to topic {}", messageCount, writeTopic);
        Properties kafkaProps = getKafkaProperties(environment);

        try (var producer = new KafkaProducer<String, String>(kafkaProps)) {
            for (int i = 0; i < messageCount; i++) {
                ObjectNode message = objectMapper.createObjectNode();
                message.put("uid", uid);
                message.put("counter", i);

                String jsonMessage = objectMapper.writeValueAsString(message);
                ProducerRecord<String, String> record = new ProducerRecord<>(writeTopic, jsonMessage);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Error sending message to Kafka", exception);
                    } else {
                        logger.info("Sent message {} to topic {}", jsonMessage, writeTopic);
                    }
                }).get(1000, TimeUnit.MILLISECONDS);
            }
            return true;
        } catch (Exception e) {
            logger.error("Error pushing messages to Kafka topic", e);
            return false;
        }
    }

    public List<Message> receiveMessages(String readTopic, int idleTimeoutMsec, int count) {
        logger.info("Reading {} messages from topic {} with timeout {} msec", count, readTopic, idleTimeoutMsec);
        Properties kafkaProps = getKafkaProperties(environment);
        List<Message> messages = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        long lastTime;

        try (var consumer = new KafkaConsumer<String, String>(kafkaProps)) {
            consumer.subscribe(Collections.singletonList(readTopic));
            lastTime = System.currentTimeMillis();

            while (System.currentTimeMillis() - lastTime < idleTimeoutMsec && messages.size() < count) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // Create a Message object directly from the JSON string
                        Message message = new Message(record.value());
                        messages.add(message);
                        logger.debug("Created Message object from: {}", record.value());
                        lastTime = System.currentTimeMillis();
                        
                        // Break if we've received the requested number of messages
                        if (messages.size() >= count) {
                            break;
                        }
                    } catch (Exception e) {
                        logger.error("Error creating Message from Kafka record: {}", record.value(), e);
                    }
                }
            }

            logger.info("Returned {}/{} messages in {} ms, (timeout: {} ms)",
                    messages.size(), count, System.currentTimeMillis() - startTime, idleTimeoutMsec);
            return messages;
        } catch (Exception e) {
            logger.error("Error receiving messages from Kafka topic", e);
            return new ArrayList<>();
        }
    }
}
