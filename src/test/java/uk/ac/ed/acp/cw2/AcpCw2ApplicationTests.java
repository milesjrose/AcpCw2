package uk.ac.ed.acp.cw2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import uk.ac.ed.acp.cw2.model.MessageRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ed.acp.cw2.service.RabbitMqService;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AcpCw2ApplicationTests {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String queueName = "test-queue";// + System.currentTimeMillis();
    private static final String topicName = "test-topic";// + System.currentTimeMillis();

    private String getBaseUrl() {return "http://localhost:" + port;}
    private String getRabbitUrl() {return getBaseUrl() + "/rabbitmq";}
    private String getKafkaUrl() {return getBaseUrl() + "/kafka";}
    private static final String uid = "s2093547";
    private static final Logger logger = LoggerFactory.getLogger(AcpCw2ApplicationTests.class);

    @Test
    void testRabbitMQ() throws Exception {
        logger.info("--------------STARTING RABBIT MQ TEST-------------");
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
        request.readTopic = topicName;
        request.writeQueueGood = "good-queue";
        request.writeQueueBad = "bad-queue";
        request.messageCount = 5;

        ResponseEntity<Void> response = restTemplate.exchange(
                getBaseUrl() + "/proccessMessages",
                HttpMethod.POST,
                new HttpEntity<>(request),
                Void.class
        );

        assertEquals(HttpStatusCode.valueOf(200), response.getStatusCode());
    }
}