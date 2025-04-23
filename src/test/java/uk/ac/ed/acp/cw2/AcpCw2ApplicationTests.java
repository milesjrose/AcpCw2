package uk.ac.ed.acp.cw2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ed.acp.cw2.domain.TranDecoder;
import uk.ac.ed.acp.cw2.model.ProcessRequest;
import uk.ac.ed.acp.cw2.model.TransformMessage;
import uk.ac.ed.acp.cw2.service.MessageTransformer;
import uk.ac.ed.acp.cw2.service.MongoDbService;
import uk.ac.ed.acp.cw2.utilities.RandomGenerator;
import uk.ac.ed.acp.cw2.model.TransformRequest;
import uk.ac.ed.acp.cw2.service.RabbitMqService;
import uk.ac.ed.acp.cw2.utilities.cacheEntry;
import uk.ac.ed.acp.cw2.utilities.local;
import jakarta.annotation.PostConstruct;
import uk.ac.ed.acp.cw2.model.BlobPacket;
import uk.ac.ed.acp.cw2.service.StorageService;
import java.util.ArrayList;
import java.util.List;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import uk.ac.ed.acp.cw2.utilities.PacketGenerator;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class AcpCw2ApplicationTests {

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private RabbitMqService rabbitMqService;

    @Autowired
    private MongoDbService mongoDbService;

    @Autowired
    private StorageService storageService;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private local http;

    @PostConstruct
    public void init() {
        this.http = new local(restTemplate, "http://localhost:" + port);
    }

    private static final String uid = "s2093547";
    private static final Logger logger = LoggerFactory.getLogger(AcpCw2ApplicationTests.class);

    @Test
    void testRabbitMQ() throws Exception {
        logger.info("--------------STARTING RABBIT MQ TEST-------------");
        String queueName = RandomGenerator.generateRandomKey("RabbitMQ");
        logger.info("queueName: {}", queueName);
        int messageCount = 5;

        http.pushRabbit(queueName, messageCount);
        List<String> messages = http.recRabbit(queueName, 1000);

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
    void testStorageService() throws Exception {
        logger.info("-------------- STORAGE SERVICE TEST ----------------");

        logger.info("Pushing blob to storage service");
        String data = RandomGenerator.generateRandomKey("store-test");
        String datasetName = RandomGenerator.generateRandomKey("store-test");
        BlobPacket packet = storageService.pushBlob(datasetName, data);
        assertNotNull(packet);

        logger.info("Receiving blob from storage service");
        BlobPacket receivedPacket = storageService.receiveBlob(packet.uuid);
        assertNotNull(receivedPacket);
        assertEquals(packet.uuid, receivedPacket.uuid);
        assertEquals(packet.datasetName, receivedPacket.datasetName);
        assertEquals(packet.data, receivedPacket.data);

        logger.info("Deleting blob from storage service");
        boolean deleted = storageService.deleteBlob(packet.uuid);
        assertTrue(deleted);

        logger.info("Checking if blob is deleted");
        BlobPacket deletedPacket = storageService.receiveBlob(packet.uuid);
        assertNull(deletedPacket);
    }

    @Test
    void testKafkaSent() throws Exception {
        logger.info("-----------STARTING KAFKA TEST-------------");
        String topicName = RandomGenerator.generateRandomKey("KafkaSent");
        logger.info("topicName: {}", topicName);
        int messageCount = 5;

        // Send kafka
        http.pushKafka(topicName, messageCount);

        // Receive kafka
        int timeoutInMsec = 5000;
        List<String> messages = http.recKafka(topicName, timeoutInMsec);
        assertNotNull(messages);
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
        String topicName = RandomGenerator.generateRandomKey("KafkaMultipleReads");
        logger.info("topicName: {}", topicName);
        int messageCount = 100;

        // Send kafka
        http.pushKafka(topicName, messageCount);

        // Rec kafka
        int timeoutInMsec = 5000;
        long startTime;
        for (int i = 0; i < 3; i++) {
            startTime = System.currentTimeMillis();
            List<String> messages = http.recKafka(topicName, timeoutInMsec);
            assertNotNull(messages);
            assertEquals(messageCount, messages.size(), "Incorrect number of messages received from Kafka");
            logger.info("[{}] MessageCount: {}, readMessages: {}, timeout: {}, time: {}", i, messageCount, messages.size(), timeoutInMsec, (System.currentTimeMillis() - startTime));
        }
    }

    @Test
    void testService() throws Exception {
        logger.info("--------------STARTING SERVICE TEST----------------");
        ProcessRequest request = new ProcessRequest();
        request.readTopic = RandomGenerator.generateRandomKey("ServiceTestRead");
        request.writeQueueGood = RandomGenerator.generateRandomKey("ServiceTestGood");
        request.writeQueueBad = RandomGenerator.generateRandomKey("ServiceTestBad");
        request.messageCount = 10;
        logger.info("readTopic: {}, Good: {}, Bad: {}, Count: {}", request.readTopic, request.writeQueueGood, request.writeQueueBad, request.messageCount);

        // Send test data
        PacketGenerator.PacketListResult packetList = PacketGenerator.generatePacketList(request.messageCount);
        String json = packetList.jsonList;
        logger.info("Created {} good messages, {} bad messages",packetList.goodTotals.size(), packetList.badTotals.size());
        http.pushKafka(request.readTopic, json);
        // Proccess messages
        http.procMsg(request);

        logger.info("-------------- RECEIVING MESSAGES ----------------");
        // Rec messages
        List<String> messages = http.recRabbit(request.writeQueueGood, 500);
        List<String> badMessages = http.recRabbit(request.writeQueueBad, 500);

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

        logger.info("-------------- GOOD QUEUE ----------------");
        for (int i = 0; i < messages.size()-1; i++) {
            // Get message
            String message = messages.get(i);
            JsonNode jsonNode = objectMapper.readTree(message);
            assertEquals(uid, jsonNode.get("uid").asText(), String.format("Unexpected uid %s in message %d", uid, i));
            String key = jsonNode.get("key").asText();
            assertTrue(key.length()==3 || key.length()==4, String.format("Unexpected key length %s in message %d", key, i));
            String uuid = jsonNode.get("uuid").asText();

            // Check cached message
            String storedMessage = http.recCache(uuid);
            JsonNode storedNode = objectMapper.readTree(storedMessage);
            assertEquals(jsonNode.get("key").asText(), storedNode.get("key").asText(), String.format("Unexpected key %s in message %d", storedNode.get("key").asText(), i));
            assertEquals(jsonNode.get("value").asDouble(), storedNode.get("value").asDouble(), String.format("Unexpected value %s in message %d", storedNode.get("value").asText(), i));
            assertEquals(jsonNode.get("uid").asText(), storedNode.get("uid").asText(), String.format("Unexpected uid %s in message %d", storedNode.get("uid").asText(), i));

        }

        // Check last packet
        assertEquals(messages.size(), packetList.goodTotals.size() + 1);
        int gindex = messages.size() - 2;
        String lastMessage = messages.get(messages.size()-1);
        assertEquals(lastMessage, "{\"TOTAL\":"+ packetList.goodTotals.get(gindex) +"}", String.format("Unexpected total %s in message %d", lastMessage, messages.size() - 1));
        logger.info("Good packets passed");


        logger.info("-------------- BAD QUEUE ----------------");
        for (int i = 0; i < badMessages.size()-1; i++) {
            // Get message
            String message = badMessages.get(i);
            JsonNode jsonNode = objectMapper.readTree(message);
            assertEquals(uid, jsonNode.get("uid").asText(), String.format("Unexpected uid %s in message %d", uid, i));
            String key = jsonNode.get("key").asText();
            assertTrue(key.length()!=3 && key.length()!=4 , String.format("Unexpected key length %s in message %d", key, i));
        }

        // Check last packet
        assertEquals(badMessages.size(), packetList.badTotals.size() + 1);
        String lastBadMessage = badMessages.get(badMessages.size()-1);
        int index = packetList.badTotals.size() - 1;
        assertEquals(lastBadMessage, "{\"TOTAL\":"+ packetList.badTotals.get(index) +"}", String.format("Unexpected total %s in message %d", lastBadMessage, badMessages.size() - 1));
        logger.info("Bad packets passed");
    }

    @Test
    void testTransform() throws Exception {
        logger.info("--------------STARTING TRANSFORM TEST----------------");
        TransformRequest request = PacketGenerator.transformRequest();

        // Push test data to queue
        PacketGenerator.TransformData data = PacketGenerator.generateTransformMessage(10);
        rabbitMqService.pushMessages(request.readQueue, data.getPackets());
        // Hit endpoint
        http.tranMsg(request);
        // get trans messages
        List<String> messages = http.recRabbit(request.writeQueue, 500);
        // Get all cache entries
        List<cacheEntry> entries = http.clearCacheEntries(data.getMessages(), mongoDbService);


        // Now do again, but keep the object to compare.
        TransformRequest request1 = PacketGenerator.transformRequest();
        request1.messageCount = request.messageCount;
        rabbitMqService.pushMessages(request1.readQueue, data.getPackets());
        // Perform ourselves to keep the transformer object
        logger.info("Transforming messages; read_queue:{}, write_queue:{}, count:{}",
                request1.readQueue, request1.writeQueue, request1.messageCount);

        List<String> messageStrings = rabbitMqService.receiveFromQueue(request1.readQueue, 500);
        List<TransformMessage> msgObjects = new ArrayList<>();
        TranDecoder decoder = new TranDecoder();
        for (String messageString : messageStrings){
            try {
                msgObjects.add(decoder.decode(messageString));
            } catch (Exception e) {
                logger.error("Error decoding packet {}", messageString);
            }
        }
        logger.info("Handing {} messages to the transformer", msgObjects.size());
        MessageTransformer transformer = new MessageTransformer(request1, msgObjects, mongoDbService, rabbitMqService);
        transformer.processMessages();
        // Receive these messages
        List<String> manual_messages = http.recRabbit(request1.writeQueue, 500);

        // Now compare
        for (int i = 0; i < messages.size(); i++) {
            assertEquals(messages.get(i), manual_messages.get(i), String.format("Returned differing messages at index: %d -> original packet:%s", i, data.getPackets().get(i).toString()));
        }
        for (int i = 0; i < entries.size(); i++) {
            assertEquals(transformer.getMessages().get(i), data.getMessages().get(i), String.format("Returned differing messages at index: %d -> original packet:%s", i, data.getPackets().get(i).toString()));
        }
        assertEquals(transformer.getTotalValueWritten(), data.getTotalValueWritten());
        assertEquals(transformer.getTotalMessagesWritten(), data.getTotalMessagesWritten());
        assertEquals(transformer.getTotalMessagesProcessed(), data.getTotalMessagesProcessed());
        assertEquals(transformer.getTotalRedisUpdates(), data.getTotalRedisUpdates());
        assertEquals(transformer.getTotalAdded(), data.getTotalAdded());
    }
}