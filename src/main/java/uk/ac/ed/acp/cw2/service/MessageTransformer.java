package uk.ac.ed.acp.cw2.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ed.acp.cw2.model.*;

import java.util.List;

public class MessageTransformer {
    private static final Logger logger = LoggerFactory.getLogger(MessageTransformer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    @Getter
    private List<TransformMessage> messages;
    private MongoDbService mongoDbService;
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
    private TransformRequest transformRequest;


    public MessageTransformer(TransformRequest transformRequest, List<TransformMessage> messages, MongoDbService mongoDbService, RabbitMqService rabbitMqService){
        this.totalMessagesWritten = 0;
        this.totalMessagesProcessed = 0;
        this.totalRedisUpdates = 0;
        this.totalValueWritten = 0.0f;
        this.totalAdded = 0.0f;
        this.messages = messages;
        this.mongoDbService = mongoDbService;
        this.rabbitMqService = rabbitMqService;
        this.transformRequest = transformRequest;
    }

    public void processMessages(){
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
        if (mongoDbService.checkKey(message.key)){
            TransformNormal cacheMessage = getCachedMessage(message.key);
            if (cacheMessage == null || message.version > cacheMessage.version){
                storeAndIncrement(message);
            }
        } else {
            storeAndIncrement(message);
        }
        queueMessage(message);
    }

    private void processMessage(TransformTombstone message){
        if (mongoDbService.checkKey(message.key)){
            mongoDbService.removeFromCache(message.key);
            totalRedisUpdates++;
        }
        totalValueWritten += message.value;
        queueTombstone(message);
    }


    private void storeAndIncrement(TransformNormal message){
        mongoDbService.storeInCache(message.key, message.toJson(objectMapper));
        totalRedisUpdates++;
        message.value += 10.5f;
        totalAdded += 10.5f;
    }

    private TransformNormal getCachedMessage(String key) {
        TransformNormal message;
        String cacheValue = mongoDbService.retrieveFromCache(key);
        try {
            message = objectMapper.readValue(cacheValue, TransformNormal.class);
        }
        catch (Exception e) {
            logger.error("Error decoding json from cache, key: {}", key);
            message = null;
        }
        return message;
    }

    private void queueMessage(TransformMessage message){
        rabbitMqService.pushMessage(transformRequest.writeQueue, message.toJson(objectMapper));
        totalValueWritten += message.value;
        totalMessagesWritten++;
    }



    private void queueTombstone(TransformTombstone message){
        OutboundTombstone outboundTombstone = new OutboundTombstone(totalMessagesWritten, totalMessagesProcessed, totalRedisUpdates, totalValueWritten, totalAdded);
        rabbitMqService.pushMessage(transformRequest.writeQueue, outboundTombstone.toJson(objectMapper));
    }


}
