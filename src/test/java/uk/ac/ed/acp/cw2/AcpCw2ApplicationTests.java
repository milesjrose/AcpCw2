package uk.ac.ed.acp.cw2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ed.acp.cw2.model.ProcessRequest;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Random;
import java.util.ArrayList;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AcpCw2ApplicationTests {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private String getBaseUrl() {return "http://localhost:" + port;}
    private String getRabbitUrl() {return getBaseUrl() + "/rabbitmq";}
    private String getKafkaUrl() {return getBaseUrl() + "/kafka";}
    private static final String uid = "s2093547";
    private static final Logger logger = LoggerFactory.getLogger(AcpCw2ApplicationTests.class);

    @Test
    void testRabbitMQ() throws Exception {
        logger.info("--------------STARTING RABBIT MQ TEST-------------");
        String queueName = generateRandomKey("RabbitMQ");
        logger.info("queueName: {}", queueName);
        int messageCount = 5;
        ResponseEntity<Void> sendResponse = restTemplate.exchange(
                getRabbitUrl() + "/" + queueName + "/" + messageCount,
                HttpMethod.PUT,
                null,
                Void.class
        );

        assertEquals(HttpStatusCode.valueOf(200), sendResponse.getStatusCode());

        int timeoutInMsec = 1000;
        ResponseEntity<List> receiveResponse = restTemplate.getForEntity(
                getRabbitUrl() + "/" + queueName + "/" + timeoutInMsec,
                List.class
        );
        
        assertEquals(HttpStatusCode.valueOf(200), receiveResponse.getStatusCode());
        assertNotNull(receiveResponse.getBody());
        
        // Extract and verify JSON fields from the response
        List<String> messages = receiveResponse.getBody();
        assertFalse(messages.isEmpty(), "No messages received from RabbitMQ");
        
        // Parse the first message to extract uid and counter
        String firstMessage = messages.get(0);
        JsonNode jsonNode = objectMapper.readTree(firstMessage);
        
        // Extract and verify the uid field
        String messageUid = jsonNode.get("uid").asText();
        assertEquals(uid, messageUid, "Unexpected uid in message");
        
        // Extract and verify the counter field
        int counter = jsonNode.get("counter").asInt();
        assertTrue(counter >= 0 && counter < messageCount, 
                "Counter should be between 0 and " + (messageCount - 1) + ", but was " + counter);
        
        System.out.println("RabbitMQ Message - uid: " + messageUid + ", counter: " + counter);
    }

    @Test
    void testKafkaSent() throws Exception {
        logger.info("-----------STARTING KAFKA TEST-------------");
        String topicName = generateRandomKey("KafkaSent");
        logger.info("topicName: {}", topicName);
        int messageCount = 5;

        ResponseEntity<Void> sendResponse = restTemplate.exchange(
                getKafkaUrl() + "/" + topicName + "/" + messageCount,
                HttpMethod.PUT,
                null,
                Void.class
        );

        assertEquals(HttpStatusCode.valueOf(200), sendResponse.getStatusCode());

        int timeoutInMsec = 5000;

        ResponseEntity<List> receiveResponse = restTemplate.getForEntity(
                getKafkaUrl() + "/" + topicName + "/" + timeoutInMsec,
                List.class
        );

        assertEquals(HttpStatusCode.valueOf(200), receiveResponse.getStatusCode());
        assertNotNull(receiveResponse.getBody());
        
        // Extract and verify JSON fields from the response
        List<String> messages = receiveResponse.getBody();
        assertFalse(messages.isEmpty(), "No messages received from Kafka");
        
        // Parse the first message to extract uid and counter
        String firstMessage = messages.get(0);
        JsonNode jsonNode = objectMapper.readTree(firstMessage);
        
        // Extract and verify the uid field
        String messageUid = jsonNode.get("uid").asText();
        assertEquals(uid, messageUid, "Unexpected uid in message");
        
        // Extract and verify the counter field
        int counter = jsonNode.get("counter").asInt();
        assertTrue(counter >= 0 && counter < messageCount, 
                "Counter should be between 0 and " + (messageCount - 1) + ", but was " + counter);
        
        System.out.println("Kafka Message - uid: " + messageUid + ", counter: " + counter);
    }

    @Test
    void testKafkaMultipleReads() throws Exception {
        logger.info("--------------STARTING KAFKA MULTIPLE READS TEST----------------");
        String topicName = generateRandomKey("KafkaMultipleReads");
        logger.info("topicName: {}", topicName);
        int messageCount = 100;

        ResponseEntity<Void> sendResponse = restTemplate.exchange(
                getKafkaUrl() + "/" + topicName + "/" + messageCount,
                HttpMethod.PUT,
                null,
                Void.class
        );

        assertEquals(HttpStatusCode.valueOf(200), sendResponse.getStatusCode(), "Failed to send messages to Kafka");

        int timeoutInMsec = 5000;
        long startTime;
        for (int i = 0; i < 3; i++) {
                startTime = System.currentTimeMillis();
                ResponseEntity<List> receiveResponse = restTemplate.getForEntity(
                        getKafkaUrl() + "/" + topicName + "/" + timeoutInMsec,
                        List.class
                );

                assertEquals(HttpStatusCode.valueOf(200), receiveResponse.getStatusCode(), "Failed to receive messages from Kafka");
                assertEquals(messageCount, receiveResponse.getBody().size(), "Incorrect number of messages received from Kafka");
                logger.info("[{}] MessageCount: {}, readMessages: {}, timeout: {}, time: {}", i, messageCount, receiveResponse.getBody().size(), timeoutInMsec, (System.currentTimeMillis() - startTime));
        }
    }

    @Test
    void testService() throws Exception {
        logger.info("--------------STARTING SERVICE TEST----------------");
        ProcessRequest request = new ProcessRequest();
        request.readTopic = generateRandomKey("ServiceTestRead");
        request.writeQueueGood = generateRandomKey("ServiceTestGood");
        request.writeQueueBad = generateRandomKey("ServiceTestBad");
        request.messageCount = 10;
        logger.info("readTopic: {}, Good: {}, Bad: {}, Count: {}", request.readTopic, request.writeQueueGood, request.writeQueueBad, request.messageCount);
        
        PacketListResult packetList = generatePacketList(request.messageCount);
        String json = packetList.jsonList;

        ResponseEntity<Void> sendResponse = restTemplate.exchange(
                getKafkaUrl() + "/sendMessage/" + request.readTopic,
                HttpMethod.POST,
                new HttpEntity<>(json, createJsonHeaders()),
                Void.class
        );

        assertEquals(HttpStatusCode.valueOf(200), sendResponse.getStatusCode(), "Failed to send messages to Kafka");

        ResponseEntity<Void> response = restTemplate.exchange(
                getBaseUrl() + "/processMessages",
                HttpMethod.POST,
                new HttpEntity<>(request),
                Void.class
        );

        logger.info("-------------- RECEIVING MESSAGES ----------------");
        // Check good queue:
        ResponseEntity<List> receiveResponse = restTemplate.getForEntity(
                getRabbitUrl() + "/" + request.writeQueueGood + "/" + 1000,
                List.class
        );

        assertEquals(HttpStatusCode.valueOf(200), receiveResponse.getStatusCode(), "Failed to receive messages from Good RabbitMQ");
        assertNotNull(receiveResponse.getBody(), "No messages received from Good RabbitMQ");
        List<String> messages = receiveResponse.getBody();
        assertFalse(messages.isEmpty(), "No messages received from Good RabbitMQ");


        // Check bad queue:
        ResponseEntity<List> receiveBadResponse = restTemplate.getForEntity(
                getRabbitUrl() + "/" + request.writeQueueBad + "/" + 1000,
                List.class
        );

        assertEquals(HttpStatusCode.valueOf(200), receiveBadResponse.getStatusCode(), "Failed to receive messages from Bad RabbitMQ");
        assertNotNull(receiveBadResponse.getBody(), "No messages received from Bad RabbitMQ");
        List<String> badMessages = receiveBadResponse.getBody();
        assertFalse(badMessages.isEmpty(), "No messages received from Bad RabbitMQ");

        // Write test data to log file before assertions
        try (PrintWriter writer = new PrintWriter(new FileWriter("service-test.log"))) {
            writer.println("=== SERVICE TEST DATA ===");
            writer.println("\n=== SENT JSON ===");
            writer.println(json);
            
            writer.println("\n=== GOOD PACKET TOTALS ===");
            for (int i = 0; i < packetList.goodTotals.size(); i++) {
                writer.printf("Index %d: %.2f%n", i, packetList.goodTotals.get(i));
            }
            
            writer.println("\n=== BAD PACKET TOTALS ===");
            for (int i = 0; i < packetList.badTotals.size(); i++) {
                writer.printf("Index %d: %.2f%n", i, packetList.badTotals.get(i));
            }
            
            writer.println("\n=== GOOD QUEUE MESSAGES ===");
            for (String message : messages) {
                writer.println(message);
            }
            
            writer.println("\n=== BAD QUEUE MESSAGES ===");
            for (String message : badMessages) {
                writer.println(message);
            }
            writer.println("--------------------------------");
        } catch (IOException e) {
            logger.error("Failed to write test data to log file", e);
        }
        

        assertEquals(messages.size()-1, packetList.goodTotals.size(), "Incorrect number of messages received from Good RabbitMQ");
        assertEquals(badMessages.size()-1, packetList.badTotals.size(), "Incorrect number of messages received from Bad RabbitMQ");
        assertEquals(HttpStatusCode.valueOf(200), response.getStatusCode(), "Failed to process messages");

        try {
            // pause for 2 seconds
            Thread.sleep(10_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();  // restore interrupt status
            // handle interruption if needed
        }

        logger.info("-------------- GOOD QUEUE ----------------");

        for (int i = 0; i < messages.size()-1; i++) {
            String message = messages.get(i);
            JsonNode jsonNode = objectMapper.readTree(message);
            assertEquals(uid, jsonNode.get("uid").asText(), String.format("Unexpected uid %s in message %d", uid, i));
            String key = jsonNode.get("key").asText();
            assertTrue(key.length()==3 || key.length()==4, String.format("Unexpected key length %s in message %d", key, i));
            String uuid = jsonNode.get("uuid").asText();

            ResponseEntity<String> storeResponse = restTemplate.exchange(
                getBaseUrl() + "/mongodb/cache/" + uuid,
                HttpMethod.GET,
                null,
                String.class
            );

            assertEquals(HttpStatusCode.valueOf(200), storeResponse.getStatusCode(), "Failed to load message from MongoDB");
            String storedMessage = storeResponse.getBody();
            JsonNode storedNode = objectMapper.readTree(storedMessage);
            assertEquals(jsonNode.get("key").asText(), storedNode.get("key").asText(), String.format("Unexpected key %s in message %d", storedNode.get("key").asText(), i));
            assertEquals(jsonNode.get("value").asDouble(), storedNode.get("value").asDouble(), String.format("Unexpected value %s in message %d", storedNode.get("value").asText(), i));
            assertEquals(jsonNode.get("uid").asText(), storedNode.get("uid").asText(), String.format("Unexpected uid %s in message %d", storedNode.get("uid").asText(), i));

        }

        assertEquals(messages.size(), packetList.goodTotals.size() + 1);
        int gindex = messages.size() - 2;
        String lastMessage = messages.get(messages.size()-1);
        assertEquals(lastMessage, "{\"TOTAL\":"+ packetList.goodTotals.get(gindex) +"}", String.format("Unexpected total %s in message %d", lastMessage, messages.size() - 1));
        


        logger.info("-------------- BAD QUEUE ----------------");

        for (int i = 0; i < badMessages.size()-1; i++) {
            String message = badMessages.get(i);
            JsonNode jsonNode = objectMapper.readTree(message);
            assertEquals(uid, jsonNode.get("uid").asText(), String.format("Unexpected uid %s in message %d", uid, i));
            String key = jsonNode.get("key").asText();
            assertTrue(key.length()!=3 && key.length()!=4 , String.format("Unexpected key length %s in message %d", key, i));
        }

        assertEquals(badMessages.size(), packetList.badTotals.size() + 1);
        String lastBadMessage = badMessages.get(badMessages.size()-1);
        int index = packetList.badTotals.size() - 1;
        assertEquals(lastBadMessage, "{\"TOTAL\":"+ packetList.badTotals.get(index) +"}", String.format("Unexpected total %s in message %d", lastBadMessage, badMessages.size() - 1));
    }
        
        
        
        
        

    /**
     * Generates a good packet JSON with a 3-4 character key
     * @return JSON string representing a good packet
     */
    private String generateGoodPacketJson() {
        Random random = new Random();
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        // Randomly choose between 3 or 4 characters
        int length = random.nextInt(2) + 3; // Will be either 3 or 4
        StringBuilder key = new StringBuilder();
        for (int i = 0; i < length; i++) {
            key.append(chars.charAt(random.nextInt(chars.length())));
        }
        float value = random.nextFloat() * 50;
        
        return String.format(
            "{\"uid\": \"s2093547\", \"key\": \"%s\", \"comment\": \" \", \"value\": %.2f}",
            key.toString(), value
        );
    }

    /**
     * Generates a bad packet JSON with a 5 character key
     * @return JSON string representing a bad packet
     */
    private String generateBadPacketJson() {
        Random random = new Random();
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        StringBuilder key = new StringBuilder();
        // Always generate 5 characters for bad packets
        for (int i = 0; i < 5; i++) {
            key.append(chars.charAt(random.nextInt(chars.length())));
        }
        float value = random.nextFloat() * 50;
        
        return String.format(
            "{\"uid\": \"s2093547\", \"key\": \"%s\", \"comment\": \" \", \"value\": %.2f}",
            key.toString(), value
        );
    }


    private static class PacketListResult {
        String jsonList;
        List<Float> goodTotals;
        List<Float> badTotals;
        List<Integer> packetTypes;
    }

    private PacketListResult generatePacketList(int numPackets) {
        Random random = new Random();
        List<String> packets = new ArrayList<>();
        List<Float> goodTotals = new ArrayList<>();
        List<Float> badTotals = new ArrayList<>();
        List<Integer> packetTypes = new ArrayList<>();
        
        float goodRunningTotal = 0;
        float badRunningTotal = 0;
        
        for (int i = 0; i < numPackets; i++) {
            // Randomly decide whether to add a good or bad packet
            boolean isGoodPacket = random.nextBoolean();
            String packet;
            float value;
            
            if (isGoodPacket) {
                packet = generateGoodPacketJson();
                // Extract value from the good packet
                value = Float.parseFloat(packet.split("\"value\": ")[1].replace("}", "").trim());
                goodRunningTotal += value;
                goodTotals.add(goodRunningTotal);
                packetTypes.add(1);
            } else {
                packet = generateBadPacketJson();
                // Extract value from the bad packet
                value = Float.parseFloat(packet.split("\"value\": ")[1].replace("}", "").trim());
                badRunningTotal += value;
                badTotals.add(badRunningTotal);
                packetTypes.add(0);
            }
            
            packets.add(packet);
        }
        
        // Create the JSON list string
        String jsonList = "[" + String.join(", ", packets) + "]";
        
        PacketListResult result = new PacketListResult();
        result.jsonList = jsonList;
        result.goodTotals = goodTotals;
        result.badTotals = badTotals;
        result.packetTypes = packetTypes;
        
        return result;
    }

    public String generateRandomKey(String prefix) {
        // Generate a random 5-digit number for the first part
        int numericPart = 10000 + new Random().nextInt(90000);

        // Generate a random 5-character alphanumeric string for the second part
        String alphanumericChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder alphanumericPart = new StringBuilder();
        Random random = new Random();

        for (int i = 0; i < 5; i++) {
            int index = random.nextInt(alphanumericChars.length());
            alphanumericPart.append(alphanumericChars.charAt(index));
        }

        // Combine the parts with a hyphen
        return prefix + "-" + numericPart + "-" + alphanumericPart.toString();
    }

    /**
     * Creates HTTP headers with Content-Type set to application/json
     */
    private HttpHeaders createJsonHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return headers;
    }
}