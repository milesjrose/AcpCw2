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
import uk.ac.ed.acp.cw2.utilities.scribe;

import uk.ac.ed.acp.cw2.model.ProcessRequest;
import uk.ac.ed.acp.cw2.service.MessageTransformer;
import uk.ac.ed.acp.cw2.service.CacheService;
import uk.ac.ed.acp.cw2.utilities.RandomGenerator;
import uk.ac.ed.acp.cw2.model.TransformRequest;
import uk.ac.ed.acp.cw2.service.RabbitMqService;
import uk.ac.ed.acp.cw2.utilities.cacheEntry;
import uk.ac.ed.acp.cw2.utilities.TranTestData;
import uk.ac.ed.acp.cw2.utilities.local;
import jakarta.annotation.PostConstruct;
import uk.ac.ed.acp.cw2.model.BlobPacket;
import uk.ac.ed.acp.cw2.service.StorageService;
import java.util.List;
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
    private CacheService cacheService;

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
        logger.info("========================PASSED=========================");
    }

    @Test
    void testStorageService() throws Exception {
        logger.info("-------------- STORAGE SERVICE TEST ----------------");

        logger.info("Pushing blob to storage service");
        String data = RandomGenerator.generateRandomKey("store-test");
        String datasetName = RandomGenerator.generateRandomKey("store-test");
        String uuid = storageService.pushBlob(datasetName, data);
        assertNotNull(uuid);

        logger.info("Receiving blob from storage service");
        BlobPacket receivedPacket = storageService.receiveBlob(uuid);
        assertNotNull(receivedPacket);
        assertEquals(uuid, receivedPacket.uuid);
        assertEquals(datasetName, receivedPacket.datasetName);
        assertEquals(data, receivedPacket.data);

        logger.info("Deleting blob from storage service");
        boolean deleted = storageService.deleteBlob(uuid);
        assertTrue(deleted);

        logger.info("Checking if blob is deleted");
        BlobPacket deletedPacket = storageService.receiveBlob(uuid);
        assertNull(deletedPacket);
        logger.info("========================PASSED=========================");
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
        logger.info("========================PASSED=========================");
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
        logger.info("========================PASSED=========================");
    }

    @Test
    void testKafkaBulk() throws Exception {
        logger.info("--------------KAFKA BULK TEST----------------");
        int messageCount = 500;

        // kafka
        String topicName = RandomGenerator.generateRandomKey("BulkTest");
        logger.info("topicName: {}", topicName);
        // Send kafka
        http.pushKafka(topicName, messageCount);
        // Time kafka
        long timeout = 5000;
        long timeTaken = http.timeKafka(topicName, (int) timeout);
        logger.info("Time taken: {} ms", timeTaken);
        assertTrue(timeTaken < timeout+200, String.format("Took too long: %d>%d", timeTaken, timeout+200));
        logger.info("========================PASSED=========================");
    }

    @Test
    void testKafkaShort() throws Exception {
        logger.info("--------------KAFKA SHORT TEST----------------");
        for (int i = 0; i < 5; i++) {
            // Setup
            int messageCount = 500;
            String topicName = RandomGenerator.generateRandomKey("ShortTest");
            logger.info("topicName: {}", topicName);
            // Send kafka
            http.pushKafka(topicName, messageCount);
            // Time kafka
            long timeout = 500;
            long timeTaken = http.timeKafka(topicName, (int) timeout);
            logger.info("Time taken: {} ms", timeTaken);
            assertTrue(timeTaken < timeout+200, String.format("Took too long: %d>%d", timeTaken, timeout+200));
            Thread.sleep(1500);
            logger.info("========================PASSED=========================");
        }
    }

    @Test
    void testMinKafkaReadTime() throws Exception {
        logger.info("--------------MIN KAFKA READ TIME TEST----------------");
        String topicName = RandomGenerator.generateRandomKey("MinKafkaReadTime");
        http.pushKafka(topicName, 100);
        logger.info("topicName: {}", topicName);
        int messageCount = 100;
        // Send kafka
        http.pushKafka(topicName, messageCount);
        // Time kafka
        long timeout = 500;
        while (!http.checkKafkaResponse(topicName, (int) timeout)){
            logger.info("Timeout: " + timeout);
            timeout += 200;
            Thread.sleep(5000);
        }
        logger.info("Minimum Kafka read time: {} ms", timeout);
        scribe.logTest("KafkaMinReadTime", String.format("Minimum Kafka read time: %d ms", timeout));
        assertTrue(timeout >= 10, String.format("Minimum Kafka read time should be at least 10 ms, but was %d ms", timeout));
        logger.info("========================PASSED=========================");
    }

    @Test
    void testRabbitBulk() throws Exception {
        logger.info("--------------RABBIT BULK TEST----------------");
        int messageCount = 500;
        // rabbit
        long timeout = 500;
        String queueName = RandomGenerator.generateRandomKey("BulkRabbitTest");
        logger.info("queueName: {}", queueName);
        // Send rabbit
        http.pushRabbit(queueName, messageCount);
        // Time rabbit
        long timeTaken = http.timeRabbit(queueName, (int) timeout);
        logger.info("Time taken: {} ms",timeTaken);
        assertTrue(timeTaken < timeout+200, String.format("Took too long: %d>%d", timeTaken, timeout+200));
        logger.info("========================PASSED=========================");
    }

    @Test
    void testRabbitShort() throws Exception {
        logger.info("--------------RABBIT SHORT TEST----------------");
        for (int i = 0; i < 5; i++) {
            //Setup
            int messageCount = 500;
            long timeout = 50;
            String queueName = RandomGenerator.generateRandomKey("ShortTest");
            logger.info("queueName: {}", queueName);
            // Send rabbit
            http.pushRabbit(queueName, messageCount);
            // Time rabbit
            long timeTaken = http.timeRabbit(queueName, (int) timeout);
            logger.info("Time taken: {} ms",timeTaken);
            assertTrue(timeTaken < timeout+200, String.format("Took too long: %d>%d", timeTaken, timeout+200));
            logger.info("========================PASSED=========================");
        }
    }

    @Test
    void testRabbitMinReadTime() throws Exception {
        logger.info("--------------RABBIT MIN READ TIME TEST----------------");
        String queueName = RandomGenerator.generateRandomKey("MinRabbitReadTime");
        logger.info("queueName: {}", queueName);
        long timeout = 40;
        while (!http.checkRabbitResponse(queueName, (int) timeout)){
            http.pushRabbit(queueName, 10);
            logger.info("Timeout: " + timeout);
            timeout += 20;
            Thread.sleep(5000);
        }
        logger.info("Minimum Rabbit read time: {} ms", timeout);
        scribe.logTest("RabbitMinReadTime", String.format("Minimum Rabbit read time: %d ms", timeout));
        assertTrue(timeout >= 10, String.format("Minimum Rabbit read time should be at least 10 ms, but was %d ms", timeout));
        logger.info("========================PASSED=========================");
    }

    @Test
    void testService() throws Exception {
        logger.info("--------------STARTING SERVICE TEST----------------");
        ProcessRequest request = new ProcessRequest();
        request.readTopic = RandomGenerator.generateRandomKey("ServiceTestRead");
        request.writeQueueGood = RandomGenerator.generateRandomKey("ServiceTestGood");
        request.writeQueueBad = RandomGenerator.generateRandomKey("ServiceTestBad");
        request.messageCount = RandomGenerator.generateInteger(100, 10);
        logger.info("readTopic: {}, Good: {}, Bad: {}, Count: {}", request.readTopic, request.writeQueueGood, request.writeQueueBad, request.messageCount);

        // Send test data
        PacketGenerator.PacketListResult packetList = PacketGenerator.generatePacketList(request.messageCount);
        String json = packetList.jsonList;
        logger.info("Created {} good messages, {} bad messages",packetList.goodTotals.size(), packetList.badTotals.size());
        http.pushKafka(request.readTopic, json);
        // Proccess messages
        http.procMsg(request);

        // Rec messages
        List<String> messages = http.recRabbit(request.writeQueueGood, 500);
        List<String> badMessages = http.recRabbit(request.writeQueueBad, 500);
        // Write test data to log file before assertions
        scribe.logProc(request,json, messages, badMessages, packetList);

        // ---------Check good messages---------
        for (int i = 0; i < messages.size()-1; i++) {
            // Get message
            String message = messages.get(i);
            JsonNode jsonNode = objectMapper.readTree(message);
            assertEquals(uid, jsonNode.get("uid").asText(), String.format("Unexpected uid %s in message %d", uid, i));
            String key = jsonNode.get("key").asText();
            assertTrue(key.length()==3 || key.length()==4, String.format("Unexpected key length %s in message %d", key, i));
            String uuid = jsonNode.get("uuid").asText();

            // Check stored message
            BlobPacket storedBlob = storageService.receiveBlob(uuid);
            String storedMessage = storedBlob.data;
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

        // ---------Check bad messages---------
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
        logger.info("===============================PASSED===================================");
    }

    void testTransform(int numPackets) throws Exception {
        logger.info("--------------STARTING TRANSFORM TEST----------------");
        // Generate test data
        logger.info("Generating {} packets", numPackets);
        PacketGenerator.TranMessagesData data = PacketGenerator.generateTransformMessages(numPackets);
        // Create request and push to queue
        TransformRequest request = PacketGenerator.transformRequest(numPackets);
        rabbitMqService.pushMessages(request.readQueue, data.getPackets());
        // Hit endpoint
        http.tranMsg(request);
        // get trans messages
        List<String> messages = http.recRabbit(request.writeQueue, 1500);
        // Get all cache entries
        List<cacheEntry> entries = http.clearCacheEntries(data.getMessages(), cacheService);
        TranTestData first = new TranTestData(request, messages, entries);


        // Now do again, but keep the object to compare.
        TransformRequest request1 = PacketGenerator.transformRequest(numPackets);
        request1.messageCount = request.messageCount;
        rabbitMqService.pushMessages(request1.readQueue, data.getPackets());
        // Call ourselfs to keep the transformer object
        MessageTransformer transformer = new MessageTransformer(request1, cacheService, rabbitMqService);
        transformer.transformMessages();
        // Receive these messages
        List<String> manual_messages = http.recRabbit(request1.writeQueue, 1500);

        TranTestData second = new TranTestData(request1, manual_messages, entries);

        // log data
        scribe.logTransform(first, second, transformer, data);
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
        logger.info("===============================PASSED===================================");
    }

    @Test
    void testTransformSingle() throws Exception {
        testTransform(10);
    }

    @Test
    void testTransormMultiple() throws Exception {
        logger.info("--------------STARTING TRANSFORM MULTIPLE TEST----------------");
        for (int i = 0; i < 5; i++) {
            int numPackets = RandomGenerator.generateInteger(100,10);
            testTransform(numPackets);
        }
        logger.info("========================PASSED=========================");
    }

    @Test
    void testTransformBulk() throws Exception {
        logger.info("--------------STARTING TRANSFORM BULK TEST----------------");
        for (int i = 0; i < 5; i++) {
            testTransform(500);
        }
        logger.info("========================PASSED=========================");
    }
}