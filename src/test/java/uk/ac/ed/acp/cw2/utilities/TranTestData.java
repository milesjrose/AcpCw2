package uk.ac.ed.acp.cw2.utilities;

import uk.ac.ed.acp.cw2.model.TransformRequest;
import uk.ac.ed.acp.cw2.model.OutboundTombstone;
import java.util.List;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TranTestData {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public TransformRequest request;
    public List<String> messages;
    public List<cacheEntry> entries;
    public int totalMessagesWritten;
    public int totalMessagesProcessed;
    public int totalRedisUpdates;
    public float totalValueWritten;
    public float totalAdded;

    public TranTestData(TransformRequest request, List<String> messages, List<cacheEntry> entries){
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
            OutboundTombstone outboundTombstone = objectMapper.readValue(lastMessage, OutboundTombstone.class);
            return outboundTombstone;
        } catch (Exception e) {
            System.out.println("Error decoding last message: " + e.getMessage());
            return null;
        }
    }
}
