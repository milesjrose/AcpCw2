package uk.ac.ed.acp.cw2.Utilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import uk.ac.ed.acp.cw2.model.TransformMessage;
import uk.ac.ed.acp.cw2.model.TransformNormal;
import uk.ac.ed.acp.cw2.model.TransformTombstone;
import com.fasterxml.jackson.databind.JsonNode;

public class Parser {
    private static final Logger logger = LoggerFactory.getLogger(Parser.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String parseString(String recString) {
        if (recString == null) {
            logger.error("Null string received");
            return null;
        } else if (recString.startsWith("\"") && recString.endsWith("\"")) {
            return recString.substring(1, recString.length() - 1);
        } else {
            return recString;
        }
    }

    public static Boolean isValidMessage(String message, List<String> requiredFields) {
        try{
            ObjectNode messageNode = objectMapper.readValue(message, ObjectNode.class);
            // Check if all required fields are present
            for (String field : requiredFields) {
                if (!messageNode.has(field)) {
                    logger.debug("Message missing required field '{}': {}", field, message);
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            logger.error("Error parsing message: {}", e.getMessage());
            return false;
        }
    }

    public static TransformMessage parseTransformMessage(String messageString){
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
