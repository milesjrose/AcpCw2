package uk.ac.ed.acp.cw2.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class TransformTombstone extends TransformMessage {
    public String type(){
        return "TransformTombstone";
    }

    public TransformTombstone(){}

    public TransformTombstone(String key, Float TOTAL){
        this.key = key;
        this.value = TOTAL;
    }

    public TransformTombstone(String key){
        this.key = key;
    }

    public ObjectNode toJson(ObjectMapper objectMapper){
        ObjectNode json = objectMapper.createObjectNode();
        json.put("key", key);
        if (value != 0){
            json.put("TOTAL", value);
        }
        return json;
    }
}
