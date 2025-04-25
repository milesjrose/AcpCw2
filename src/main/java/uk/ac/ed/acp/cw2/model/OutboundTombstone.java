package uk.ac.ed.acp.cw2.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;
import lombok.Setter;

public class OutboundTombstone {
    @Setter
    @Getter
    private Integer totalMessagesWritten;
    @Setter
    @Getter
    private Integer totalMessagesProcessed;
    @Setter
    @Getter
    private Integer totalRedisUpdates;
    @Setter
    @Getter
    private Float totalValueWritten;
    @Setter
    @Getter
    private Float totalAdded;

    public OutboundTombstone(){
        this.totalMessagesWritten = 0;
        this.totalMessagesProcessed = 0;
        this.totalRedisUpdates = 0;
        this.totalValueWritten = 0f;
        this.totalAdded = 0f;
    }

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
