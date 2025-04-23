package uk.ac.ed.acp.cw2.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class TransformMessage{
    public String key;
    public Float value;

    public ObjectNode toJson(ObjectMapper objectMapper){
        ObjectNode json = objectMapper.createObjectNode();
        json.put("key", key);
        return json;
    }

    public String type(){
        return "TransformMessage";
    }
}
