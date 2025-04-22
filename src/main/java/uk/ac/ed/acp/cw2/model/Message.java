package uk.ac.ed.acp.cw2.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class Message {
    public String uid;
    public String key;
    public String comment;
    public Float value;

    public String toString(ObjectMapper objectMapper){
        ObjectNode message = objectMapper.createObjectNode();
        message.put("uid", uid);
        message.put("key", key);
        message.put("comment", comment);
        message.put("value", value);

        String jsonMessage = null;
        try {
            jsonMessage = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return jsonMessage;
    }
}
