package uk.ac.ed.acp.cw2.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import uk.ac.ed.acp.cw2.model.TransformMessage;

public static class TranDecoder {
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TransformMessage decode(String messageString){
        JsonNode jsonNode;
        try{
            jsonNode = objectMapper.readTree(messageString);
        } catch (Exception e){
            return null;
        }
        if (!jsonNode.has("Key")){
            return null;
        }
        if (jsonNode.has(""))

    }
}