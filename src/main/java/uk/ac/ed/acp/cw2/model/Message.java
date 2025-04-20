package uk.ac.ed.acp.cw2.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
        logger.debug("Creating message with string {}", jsonString);
        ObjectNode data = null;
        List<String> requiredData = Arrays.asList("uid", "key", "comment", "value");
        ArrayList<String> missingData = new ArrayList<>();
        
        // Check for missing required fields
        for (String field : requiredData){
            if (!jsonString.contains(field)){
                logger.debug("Message contains no {}", field);
                missingData.add(field);
            }
        }
        
        try {
            // Parse the original JSON
            ObjectNode originalData = (ObjectNode) objectMapper.readTree(jsonString);
            
            // Create a new ObjectNode with only the required fields
            data = objectMapper.createObjectNode();
            
            // Copy only the required fields from the original data
            for (String field : requiredData) {
                if (originalData.has(field)) {
                    data.set(field, originalData.get(field));
                } else {
                    missingData.add(field);
                }
            }
        } catch (Exception e) {
            logger.error("Error parsing JSON string: {}", jsonString, e);
            data = objectMapper.createObjectNode();
            missingData.addAll(requiredData);
        }

        jsonData = fillMissingData(data, missingData);
        logger.info("Created message with data{}", jsonData.toString());
    }

    private ObjectNode fillMissingData(ObjectNode node, List<String> requiredData){
        for (String field : requiredData){
            if (field.equals("value")){node.put(field, 1);} else{node.put(field, "missing");}
        }
        return node;
    }
    
    /**
     * Constructor that takes an ObjectNode
     */
    public Message(ObjectNode jsonNode) {
        this.jsonData = jsonNode;
    }

    public boolean checkGood(int runningTotalValue) {
        try{
            String key = getKey();
            if (key.length() == 4 || key.length() == 5) {
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

    /**
     * Returns the good JSON as an ObjectNode
     * @return ObjectNode containing the good JSON
     */
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
    
    /**
     * Returns the bad JSON as an ObjectNode
     * @return ObjectNode containing the bad JSON
     */
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
    
    /**
     * Returns the store JSON as an ObjectNode
     * @return ObjectNode containing the store JSON
     */
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
