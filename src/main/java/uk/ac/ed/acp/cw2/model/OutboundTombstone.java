package uk.ac.ed.acp.cw2.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class OutboundTombstone {
    private Integer totalMessagesWritten;
    private Integer totalMessagesProcessed;
    private Integer totalRedisUpdates;
    private Float totalValueWritten;
    private Float totalAdded;

    public OutboundTombstone(Integer totalMessagesWritten, Integer totalMessagesProcessed, Integer totalRedisUpdates, Float totalValueWritten, Float totalAdded) {
        this.totalMessagesWritten = totalMessagesWritten;
        this.totalMessagesProcessed = totalMessagesProcessed;
        this.totalRedisUpdates = totalRedisUpdates;
        this.totalValueWritten = totalValueWritten;
        this.totalAdded = totalAdded;
    }

    public ObjectNode toJson(ObjectMapper objectMapper){
        ObjectNode json = objectMapper.createObjectNode();
        json.put("totalMessagesWritten", totalMessagesWritten);
        json.put("totalMessagesProcessed", totalMessagesProcessed);
        json.put("totalRedisUpdates", totalRedisUpdates);
        json.put("totalValueWritten", totalValueWritten);
        json.put("totalAdded", totalAdded);
        return json;
    }
}
