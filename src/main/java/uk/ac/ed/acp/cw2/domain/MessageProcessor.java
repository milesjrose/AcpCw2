package uk.ac.ed.acp.cw2.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ed.acp.cw2.model.ProcessRequest;
import uk.ac.ed.acp.cw2.service.KafkaService;
import uk.ac.ed.acp.cw2.service.RabbitMqService;
import uk.ac.ed.acp.cw2.service.StorageService;

import java.util.ArrayList;
import java.util.List;

public class MessageProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static final List<String> requiredFields = List.of("uid", "key", "comment", "value");

    private ProcessRequest request;
    private List<ProcMessage> uncheckedMessages;
    private List<ProcMessage> goodMessages;
    private List<ProcMessage> badMessages;
    private float goodTotalValue;
    private float badTotalValue;
    private final StorageService storageService;
    private final RabbitMqService rabbitMqService;
    private final KafkaService kafkaService;

    public MessageProcessor(ProcessRequest request,
                            StorageService storageService,
                            RabbitMqService rabbitMqService,
                            KafkaService kafkaService) {
        this.request = request;
        this.storageService = storageService;
        this.rabbitMqService = rabbitMqService;
        this.kafkaService = kafkaService;
        this.uncheckedMessages = new ArrayList<>();
        this.goodMessages = new ArrayList<>();
        this.badMessages = new ArrayList<>();
        this.goodTotalValue = 0;
        this.badTotalValue = 0;
    }

    public void proccessMessages(){
        logger.info("Processing messages; topic:{}, good_queue:{}, bad_queue:{}, count:{}",
            request.readTopic, request.writeQueueGood, request.writeQueueBad, request.messageCount);
        receiveMessages();
        checkMessages();            // Check if the messages are good or bad, and add totals.
        queueStoreMessages();       // Store and queue good and bad messages.
        sendTotalValues();          // Send totals to queues.
    }

    // Get messages from kafka
    private void receiveMessages(){
        List<String> messages = kafkaService.receiveCount(request.readTopic, request.messageCount, requiredFields);
        for (String messageString : messages){
            try {
                ProcMessage message = new ProcMessage(messageString);
                uncheckedMessages.add(message);
            } catch (Exception e) {
                logger.error("Error creating message", e);
            }
        }
    }

    // Check if the messages are good or bad, and add totals.       
    private void checkMessages() {
        logger.info("goodTotal: {}, badTotal: {}", goodTotalValue, badTotalValue);
        try{
            for (ProcMessage message : uncheckedMessages) {
                if (message.checkGood(goodTotalValue)) {
                    goodMessages.add(message);
                goodTotalValue += message.getValue();
            } else {
                badMessages.add(message);
                badTotalValue += message.getValue();
            }
            }
            uncheckedMessages.clear();
        }
        catch (Exception e){
            logger.error("Error checking messages", e);
        }
    }

    // Store and queue good and bad messages
    private void queueStoreMessages() {
        try{
            // Store and queue good messages
            for (ProcMessage message : goodMessages) {
                message.setUuid(storeMessage(message));
        }
        queueGood();
        // Queue bad messages
        queueBad();
        // Clear lists
            goodMessages.clear();
            badMessages.clear();
        }
        catch (Exception e){
            logger.error("Error processing messages", e);
        }
    }

    // Send total values to queues
    private void sendTotalValues() {
        try{
            ObjectNode goodTotalMessage = objectMapper.createObjectNode();
            goodTotalMessage.put("TOTAL", goodTotalValue);

        ObjectNode badTotalMessage = objectMapper.createObjectNode();
        badTotalMessage.put("TOTAL", badTotalValue);

            rabbitMqService.push(request.writeQueueGood, goodTotalMessage);
            rabbitMqService.push(request.writeQueueBad, badTotalMessage);
        }
        catch (Exception e){
            logger.error("Error sending total values", e);
        }
    }

    // Store message in storage
    private String storeMessage(ProcMessage message) {
        try{
            return storageService.pushBlob(message.getStoreJsonNode());
        }
        catch (Exception e){
            logger.error("Error storing message in mongo", e);
            return null;
        }
    }

    // Queue good messages
    private void queueGood() {
        try{
            List<ObjectNode> messages = new ArrayList<>();
            for (ProcMessage message : goodMessages) {
                messages.add(message.getGoodJsonNode());
            }
            rabbitMqService.push(request.writeQueueGood, messages);
        }
        catch (Exception e){
            logger.error("Error queuing good messages", e);
        }
    }

    // Queue bad messages
    private void queueBad() {
        try{
            List<ObjectNode> messages = new ArrayList<>();
            for (ProcMessage message : badMessages) {
                messages.add(message.getBadJsonNode());
            }
            rabbitMqService.push(request.writeQueueBad, messages);
        }
        catch (Exception e){
            logger.error("Error queuing bad messages", e);
        }
    }
}
