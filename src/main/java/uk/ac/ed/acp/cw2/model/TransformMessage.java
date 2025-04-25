package uk.ac.ed.acp.cw2.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class TransformMessage{
    @JsonProperty("key")
    public String key;
    @JsonProperty("value")
    @JsonAlias("TOTAL")
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
