package uk.ac.ed.acp.cw2.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Message {
    private static final Logger logger = LoggerFactory.getLogger(Message.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    
    private final ObjectNode jsonData;
    private String uuid;
    private int runningTotalValue;

    // Getters and setters
    public void setUuid(String uuid) {this.uuid = uuid;}
    
    public String getUid() {return jsonData.get("uid").asText();}
    public String getKey() {return jsonData.get("key").asText();}
    public String getComment() {return jsonData.get("comment").asText();}
    public Integer getValue() {return jsonData.get("value").asInt();}
    
    /**
     * Constructor that takes a JSON string
     */
    public Message(String jsonString) {
        ObjectNode data;
        try {
            data = (ObjectNode) objectMapper.readTree(jsonString);
        } catch (Exception e) {
            logger.error("Error parsing JSON string: {}", jsonString, e);
            // Initialize with empty values if parsing fails
            data = objectMapper.createObjectNode();
            data.put("uid", "");
            data.put("key", "");
            data.put("comment", "");
            data.put("value", 0);
        }
        jsonData = data;
    }
    
    /**
     * Constructor that takes an ObjectNode
     */
    public Message(ObjectNode jsonNode) {
        this.jsonData = jsonNode;
    }

    public boolean checkGood(int runningTotalValue) {
        String key = getKey();
        if (key.length() == 4 || key.length() == 5) {
            this.runningTotalValue = runningTotalValue + getValue();
            return true;
        }
        return false;
    }

    /**
     * Returns the good JSON as an ObjectNode
     * @return ObjectNode containing the good JSON
     */
    public ObjectNode getGoodJsonNode() {
        // Create a copy of the base JSON object
        ObjectNode result = jsonData.deepCopy();
        // Add the uuid field
        result.put("uuid", uuid);
        return result;
    }
    
    /**
     * Returns the bad JSON as an ObjectNode
     * @return ObjectNode containing the bad JSON
     */
    public ObjectNode getBadJsonNode() {
        // For bad, just return the base JSON object
        return jsonData.deepCopy();
    }
    
    /**
     * Returns the store JSON as an ObjectNode
     * @return ObjectNode containing the store JSON
     */
    public ObjectNode getStoreJsonNode() {
        // Create a copy of the base JSON object
        ObjectNode result = jsonData.deepCopy();
        // Add the runningTotalValue field
        result.put("runningTotalValue", runningTotalValue);
        return result;
    }
}
