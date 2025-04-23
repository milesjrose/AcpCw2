package uk.ac.ed.acp.cw2.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ed.acp.cw2.model.TransformMessage;
import uk.ac.ed.acp.cw2.model.TransformNormal;
import uk.ac.ed.acp.cw2.model.TransformTombstone;
import uk.ac.ed.acp.cw2.service.MessageProcessor;

public class TranDecoder {
    private static final Logger logger = LoggerFactory.getLogger(TranDecoder.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TransformMessage decode(String messageString){
        JsonNode jsonNode;
        try{
            jsonNode = objectMapper.readTree(messageString);
        } catch (Exception e){
            logger.error("Error mapping jsonNode from {}", messageString);
            return null;
        }
        logger.debug("Processing node {}", jsonNode);
        if (jsonNode.has("key")){
            // Transform packet
            String key = jsonNode.get("key").asText();
            if (jsonNode.has("version") && jsonNode.has("value")){
                // Normal
                TransformNormal message = new TransformNormal();
                message.key = key;
                message.version = jsonNode.get("version").asInt();
                message.value = (float) jsonNode.get("value").asDouble();
                return message;
            } else {
                // Tombstone
                TransformTombstone message = new TransformTombstone();
                if (jsonNode.has("TOTAL")){
                    message.value = (float) jsonNode.get("TOTAL").asDouble();
                } else {
                    message.value = 0.0f;
                }
                message.key = jsonNode.get("key").asText();
                return message;
            }
        } else {
            throw new RuntimeException();
        }
    }
}