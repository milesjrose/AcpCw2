package uk.ac.ed.acp.cw2.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class ProcMessage {
    private static final Logger logger = LoggerFactory.getLogger(ProcMessage.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    public final ObjectNode jsonData;
    private String uuid;
    private float runningTotalValue;

    // Getters and setters
    public void setUuid(String uuid) {this.uuid = uuid;}
    
    public String getUid() {return jsonData.get("uid").asText();}
    public String getKey() {return jsonData.get("key").asText();}
    public String getComment() {return jsonData.get("comment").asText();}
    public float getValue() {return (float) jsonData.get("value").asDouble();}

    public ProcMessage(String jsonString) throws JsonProcessingException {
        logger.debug("Creating message with string {}", jsonString);
        ObjectNode data = null;
        List<String> requiredData = Arrays.asList("uid", "key", "comment", "value");

        try {
            // Parse the original JSON
            ObjectNode originalData = (ObjectNode) objectMapper.readTree(jsonString);

            // Ignore any extra fields
            data = objectMapper.createObjectNode();
            for (String field : requiredData) {
                if (originalData.has(field)) {
                    data.set(field, originalData.get(field));
                } else {
                    logger.error("Incorrect message fields");
                    throw new RuntimeException("Invalid Message");
                }
            }
        } catch (Exception e) {
            logger.error("Error parsing JSON string: {}", jsonString, e);
            throw e;
        }

        jsonData = data;
        logger.debug("Created message with data{}", jsonData);
    }

    public boolean checkGood(float runningTotalValue) {
        try{
            String key = getKey();
            if (key.length() == 3 || key.length() == 4) {
                this.runningTotalValue = runningTotalValue + getValue();
                return true;
            }
            return false;
        }
        catch (Exception e){
            logger.error("Error checking message {}", jsonData.toString(), e);
            return false;
        }
    }

    public ObjectNode getGoodJsonNode() {
        try{
            // Create a copy of the base JSON object
            ObjectNode result = jsonData.deepCopy();
            // Add the uuid field
            result.put("uuid", uuid);
            return result;
        }
        catch (Exception e){
            logger.error("Error getting good JSON node", e);
            return null;
        }
    }

    public ObjectNode getBadJsonNode() {
        try{
            // For bad, just return the base JSON object
            return jsonData.deepCopy();
        }
        catch (Exception e){
            logger.error("Error getting bad JSON node", e);
            return null;
        }
    }

    public ObjectNode getStoreJsonNode() {
        try{
            // Create a copy of the base JSON object
            ObjectNode result = jsonData.deepCopy();
            // Add the runningTotalValue field
            result.put("runningTotalValue", runningTotalValue);
            return result;
        }
        catch (Exception e){
            logger.error("Error getting store JSON node", e);
            return null;
        }
    }
}
