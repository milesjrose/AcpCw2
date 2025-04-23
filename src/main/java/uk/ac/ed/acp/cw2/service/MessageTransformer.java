package uk.ac.ed.acp.cw2.service;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ed.acp.cw2.model.*;
import uk.ac.ed.acp.cw2.Utilities.Parser;
import java.util.ArrayList;
import java.util.List;

public class MessageTransformer {
    private static final Logger logger = LoggerFactory.getLogger(MessageTransformer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final List<String> requiredFields = List.of("key");

    @Getter
    private List<TransformMessage> messages;
    private CacheService cacheService;
    private RabbitMqService rabbitMqService;
    @Getter
    private Integer totalMessagesWritten;   // Number of messages written to queue
    @Getter
    private Integer totalMessagesProcessed; // Number of messages processed
    @Getter
    private Integer totalRedisUpdates;      // Number of redis updates
    @Getter
    private Float totalValueWritten;        // Total value written to queue
    @Getter
    private Float totalAdded;               // Total value of 10.5s added to messages
    private TransformRequest request;


    public MessageTransformer(TransformRequest request, CacheService cacheService, RabbitMqService rabbitMqService){
        this.totalMessagesWritten = 0;
        this.totalMessagesProcessed = 0;
        this.totalRedisUpdates = 0;
        this.totalValueWritten = 0.0f;
        this.totalAdded = 0.0f;
        this.messages = new ArrayList<>();
        this.cacheService = cacheService;
        this.rabbitMqService = rabbitMqService;
        this.request = request;
    }

    public void transformMessages(){
        logger.info("Transforming messages; read_queue:{}, write_queue:{}, count:{}",
            request.readQueue, request.writeQueue, request.messageCount);
        receiveMessages();
        processMessages();
    }

    // ================================ Receive ================================
    private void receiveMessages(){
        // Receive messages from queue, no timeout
        List<String> messageStrings = rabbitMqService.receiveValidMessagesFromQueue(request.readQueue, request.messageCount, requiredFields);
        // Decode messages
        for (String messageString : messageStrings){
        try {
            messages.add(Parser.parseTransformMessage(messageString));
        } catch (Exception e) {
                logger.error("Error decoding packet {}", messageString);
            }
        }
    }

    // ================================ Process ================================
    private void processMessages(){
        for (TransformMessage message : messages){
            logger.debug("Transforming message {} {}", message.type(), message.toJson(objectMapper));
            if (message instanceof TransformNormal n_msg){
                processMessage(n_msg);
            } else if (message instanceof TransformTombstone t_msg){
                processMessage(t_msg);
            }
            totalMessagesProcessed++;
        }
    }

    private void processMessage(TransformNormal message){
        if (cacheService.checkKey(message.key)){
            TransformNormal cacheMessage = getCachedMessage(message.key);
            if (cacheMessage == null || message.version > cacheMessage.version){
                cache(message);
            }
        } else {
            cache(message);
        }
        queueMessage(message);
    }

    private void processMessage(TransformTombstone message){
        if (cacheService.checkKey(message.key)){
            cacheService.removeFromCache(message.key);
            totalRedisUpdates++;
        }
        totalValueWritten += message.value;
        queueTombstone(message);
    }

    // ================================ Cache ================================

    private void cache(TransformNormal message){
        cacheService.storeInCache(message.key, message.toJson(objectMapper));
        totalRedisUpdates++;
        message.value += 10.5f;
        totalAdded += 10.5f;
    }

    private TransformNormal getCachedMessage(String key) {
        TransformNormal message;
        String cacheValue = cacheService.retrieveFromCache(key);
        try {
            message = objectMapper.readValue(cacheValue, TransformNormal.class);
        }
        catch (Exception e) {
            logger.error("Error decoding json from cache, key: {}", key);
            message = null;
        }
        return message;
    }

    // ================================ Queue ================================

    private void queueMessage(TransformMessage message){
        rabbitMqService.pushMessage(request.writeQueue, message.toJson(objectMapper));
        totalValueWritten += message.value;
        totalMessagesWritten++;
    }

    private void queueTombstone(TransformTombstone message){
        OutboundTombstone outboundTombstone = new OutboundTombstone(totalMessagesWritten, totalMessagesProcessed, totalRedisUpdates, totalValueWritten, totalAdded);
        rabbitMqService.pushMessage(request.writeQueue, outboundTombstone.toJson(objectMapper));
    }


}
