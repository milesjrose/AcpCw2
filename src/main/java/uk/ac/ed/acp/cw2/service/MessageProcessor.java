package uk.ac.ed.acp.cw2.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ed.acp.cw2.domain.ProcMessage;
import uk.ac.ed.acp.cw2.model.ProcessRequest;

import java.util.ArrayList;
import java.util.List;

public class MessageProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private ProcessRequest request;
    private List<ProcMessage> uncheckedMessages;
    private List<ProcMessage> goodMessages;
    private List<ProcMessage> badMessages;
    private float goodTotalValue;
    private float badTotalValue;
    private final StorageService storageService;
    private final RabbitMqService rabbitMqService;

    public MessageProcessor(ProcessRequest request,
                            StorageService storageService,
                            RabbitMqService rabbitMqService) {
        this.request = request;
        this.storageService = storageService;
        this.rabbitMqService = rabbitMqService;
        this.uncheckedMessages = new ArrayList<>();
        this.goodMessages = new ArrayList<>();
        this.badMessages = new ArrayList<>();
        this.goodTotalValue = 0;
        this.badTotalValue = 0;
    }

    public void addMessages(List<ProcMessage> messages) {
        uncheckedMessages.addAll(messages);
    }

    public void proccessMessages(List<ProcMessage> messages){
        addMessages(messages);      // Add messages to the message processor.
        checkMessages();            // Check if the messages are good or bad, and add totals.
        queueStoreMessages();       // Store and queue good and bad messages.
        sendTotalValues();          // Send totals to queues.

    }
    public void checkMessages() {
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

    /* Store and queue good and bad messages */
    public void queueStoreMessages() {
        try{
            // Store and queue good messages
            for (ProcMessage message : goodMessages) {
                message.setUuid(storeMessageMongo(message));
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

    /* Send total values to queues*/
    public void sendTotalValues() {
        try{
            ObjectNode goodTotalMessage = objectMapper.createObjectNode();
            goodTotalMessage.put("TOTAL", goodTotalValue);

        ObjectNode badTotalMessage = objectMapper.createObjectNode();
        badTotalMessage.put("TOTAL", badTotalValue);

            rabbitMqService.pushMessage(request.writeQueueGood, goodTotalMessage);
            rabbitMqService.pushMessage(request.writeQueueBad, badTotalMessage);
        }
        catch (Exception e){
            logger.error("Error sending total values", e);
        }
    }

    /* Store message in mongo */
    private String storeMessageMongo(ProcMessage message) {
        try{
            return storageService.pushBlob(message.getStoreJsonNode());
        }
        catch (Exception e){
            logger.error("Error storing message in mongo", e);
            return null;
        }
    }

    /* Queue good messages */
    private void queueGood() {
        try{
            List<ObjectNode> messages = new ArrayList<>();
            for (ProcMessage message : goodMessages) {
                messages.add(message.getGoodJsonNode());
            }
            rabbitMqService.pushMessages(request.writeQueueGood, messages);
        }
        catch (Exception e){
            logger.error("Error queuing good messages", e);
        }
    }

    /* Queue bad messages */
    private void queueBad() {
        try{
            List<ObjectNode> messages = new ArrayList<>();
            for (ProcMessage message : badMessages) {
                messages.add(message.getBadJsonNode());
            }
            rabbitMqService.pushMessages(request.writeQueueBad, messages);
        }
        catch (Exception e){
            logger.error("Error queuing bad messages", e);
        }
    }
}
