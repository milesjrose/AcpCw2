package uk.ac.ed.acp.cw2.utilities;

import uk.ac.ed.acp.cw2.model.TransformRequest;
import uk.ac.ed.acp.cw2.model.OutboundTombstone;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;

public class TranTestData {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public TransformRequest request;
    public List<String> messages;
    public List<String> entries;
    public int totalMessagesWritten;
    public int totalMessagesProcessed;
    public int totalRedisUpdates;
    public float totalValueWritten;
    public float totalAdded;

    public TranTestData(TransformRequest request, List<String> messages, List<String> entries){
        this.request = request;
        this.messages = messages;
        this.entries = entries;
        try {
            OutboundTombstone outboundTombstone = decodeLastMessage();
            totalMessagesWritten = outboundTombstone.getTotalMessagesWritten();
            totalMessagesProcessed = outboundTombstone.getTotalMessagesProcessed();
            totalRedisUpdates = outboundTombstone.getTotalRedisUpdates();
            totalValueWritten = outboundTombstone.getTotalValueWritten();
            totalAdded = outboundTombstone.getTotalAdded();
        } catch (Exception e){
            totalMessagesWritten = 0;
            totalMessagesProcessed = 0;
            totalRedisUpdates = 0;
            totalValueWritten = 0f;
            totalAdded = 0f;
        }
    }

    public OutboundTombstone decodeLastMessage(){
        String lastMessage = messages.get(messages.size()-1);
        try {
            JsonNode node = objectMapper.readTree(lastMessage);
            OutboundTombstone msg = new OutboundTombstone();
            msg.setTotalMessagesWritten(node.get("totalMessagesWritten").asInt());
            msg.setTotalMessagesProcessed(node.get("totalMessagesProcessed").asInt());
            msg.setTotalRedisUpdates(node.get("totalRedisUpdates").asInt());
            msg.setTotalValueWritten(new BigDecimal(node.get("totalValueWritten").asText()).floatValue());
            msg.setTotalAdded(new BigDecimal(node.get("totalAdded").asText()).floatValue());
            return msg;
        } catch (Exception e) {
            System.out.println("Error decoding last message: " + e.getMessage());
            return null;
        }
    }
}
