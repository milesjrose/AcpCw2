package uk.ac.ed.acp.cw2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.*;
import uk.ac.ed.acp.cw2.model.MessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ed.acp.cw2.service.RabbitMqService;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.Random;

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
        String queueName = generateRandomKey();
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
        String topicName = generateRandomKey();
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
    void testService() throws Exception {
        logger.info("--------------STARTING SERVICE TEST----------------");
        MessageRequest request = new MessageRequest();
        request.readTopic = generateRandomKey();
        request.writeQueueGood = generateRandomKey();
        request.writeQueueBad = generateRandomKey();
        request.messageCount = 10;
        logger.info("readTopic: {}, Good: {}, Bad: {}, Count: {}", request.readTopic, request.writeQueueGood, request.writeQueueBad, request.messageCount);
        
        String json = "[" +
                "{" +
                "\"uid\": \"s2093547\"," +
                "\"key\": \"ABCD\"," +
                "\"comment\": \"\"," +
                "\"value\": 10.3" +
                "}," +
                "{" +
                "\"uid\": \"s2093547\"," +
                "\"key\": \"EFGH\"," +
                "\"comment\": \"\"," +
                "\"value\": 11.7" +
                "}," +
                "{" +
                "\"uid\": \"s2093547\"," +
                "\"key\": \"IJKL\"," +
                "\"comment\": \"\"," +
                "\"value\": 9.2" +
                "}," +
                "{" +
                "\"uid\": \"s2093547\"," +
                "\"key\": \"MNOP\"," +
                "\"comment\": \"\"," +
                "\"value\": 14.8" +
                "}," +
                "{" +
                "\"uid\": \"s2093547\"," +
                "\"key\": \"QRST\"," +
                "\"comment\": \"\"," +
                "\"value\": 7.6" +
                "}," +
                "{" +
                "\"uid\": \"s2093547\"," +
                "\"key\": \"UVWX\"," +
                "\"comment\": \"\"," +
                "\"value\": 12.4" +
                "}," +
                "{" +
                "\"uid\": \"s2093547\"," +
                "\"key\": \"YZAB\"," +
                "\"comment\": \"\"," +
                "\"value\": 8.9" +
                "}," +
                "{" +
                "\"uid\": \"s2093547\"," +
                "\"key\": \"CDEF\"," +
                "\"comment\": \"\"," +
                "\"value\": 13.1" +
                "}," +
                "{" +
                "\"uid\": \"s2093547\"," +
                "\"key\": \"GHIJ\"," +
                "\"comment\": \"\"," +
                "\"value\": 6.5" +
                "}," +
                "{" +
                "\"uid\": \"s2093547\"," +
                "\"key\": \"KLMN\"," +
                "\"comment\": \"\"," +
                "\"value\": 15.0" +
                "}," +
                "{" +
                "\"uid\": \"s2093547\"," +
                "\"key\": \"KLMARN\"," +
                "\"comment\": \"\"," +
                "\"value\": 15.0" +
                "}" +
                "]";

        ResponseEntity<Void> sendResponse = restTemplate.exchange(
                getKafkaUrl() + "/sendMessage/" + request.readTopic,
                HttpMethod.POST,
                new HttpEntity<>(json, createJsonHeaders()),
                Void.class
        );

        ResponseEntity<Void> response = restTemplate.exchange(
                getBaseUrl() + "/processMessages",
                HttpMethod.POST,
                new HttpEntity<>(request),
                Void.class
        );

        assertEquals(HttpStatusCode.valueOf(200), response.getStatusCode());
    }

    public String generateRandomKey() {
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
        return "test-" + numericPart + "-" + alphanumericPart.toString();
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