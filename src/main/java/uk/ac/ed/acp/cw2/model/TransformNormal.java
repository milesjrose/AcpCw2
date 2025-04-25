package uk.ac.ed.acp.cw2.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class TransformNormal extends TransformMessage{
    @JsonProperty("version")
    public Integer version;

    public ObjectNode toJson(ObjectMapper objectMapper){
        ObjectNode json = objectMapper.createObjectNode();
        json.put("key", key);
        json.put("version", version);
        json.put("value", value);
        return json;
    }
    public String type(){
        return "TransformNormal";
    }
}
