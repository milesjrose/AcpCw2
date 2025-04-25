package uk.ac.ed.acp.cw2.utilities;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import uk.ac.ed.acp.cw2.model.TransformMessage;
import uk.ac.ed.acp.cw2.model.TransformNormal;
import uk.ac.ed.acp.cw2.model.TransformRequest;
import uk.ac.ed.acp.cw2.model.TransformTombstone;

import java.util.Random;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import static java.lang.Math.max;


public class PacketGenerator {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static Random random = new Random();

    public static class TranMessagesData{
        @Getter
        Float totalValueWritten;
        @Getter
        Integer tombstoneCount;
        @Getter
        Integer normalCount;
        @Getter
        Integer totalRedisUpdates;
        @Getter
        Integer totalMessagesWritten;
        @Getter
        Float totalAdded;
        @Getter
        Integer totalMessagesProcessed;

        @Getter
        Float tombstoneValueWritten;
        @Getter
        List<TransformMessage> removedMessages;
        @Getter
        Integer unstoredCount;
        @Getter
        List<Integer> unstoredIndices;
        @Getter
        List<TransformMessage> unstoredMessages;
        @Getter
        List<Integer> removedMessageIndices;

        @Getter
        List<Integer> tombstoneIndices;
        List<Integer> writtenIndices;
        @Getter
        List<Integer> unWrittenIndices;
        @Getter
        List<TransformMessage> messages;
        @Getter
        List<ObjectNode> packets;

        @Getter
        List<TransformNormal> redisCache;

        public Integer messageCount(){
            return tombstoneCount + normalCount;
        }

        public void calculateUnWrittenIndices(){
            for (int i = 0; i < messageCount(); i++){
                if (!writtenIndices.contains(i)){
                    unWrittenIndices.add(i);
                }
            }
        }

        public TranMessagesData(){
            totalValueWritten = 0f;
            tombstoneCount = 0;
            normalCount = 0;
            totalRedisUpdates = 0;
            totalMessagesWritten = 0;
            totalAdded = 0f;
            totalMessagesProcessed = 0;

            tombstoneIndices = new ArrayList<>();
            messages = new ArrayList<>();
            packets = new ArrayList<>();
            unstoredIndices = new ArrayList<>();

            tombstoneValueWritten = 0f;
            removedMessages = new ArrayList<>();
            unstoredCount = 0;
            unstoredMessages = new ArrayList<>();
            removedMessageIndices = new ArrayList<>();
            writtenIndices = new ArrayList<>();
            unWrittenIndices = new ArrayList<>();
            redisCache = new ArrayList<>();
        }

        public void add(TransformNormal msg){
            if (redisContainsKey(msg.key)){
                int index = redisGetKeyIndex(msg.key);
                int cacheVersion = redisCache.get(index).version;
                if (msg.version > cacheVersion){
                    // Remove old version from cache - doesnt count in real redis, as update in place.
                    redisCache.remove(index);
                    // Add with update++
                    redisAdd(msg);
                } else {
                    unstoredCount += 1;
                    unstoredIndices.add(messages.size());
                    unstoredMessages.add(msg);
                }
            } else {
                redisAdd(msg);
            }
            // Add to queue
            queueAdd(msg.value);
            // Add to written msg/packet/indices
            messages.add(msg);
            packets.add(msg.toJson(objectMapper));
            writtenIndices.add(messages.size());
            // Update total
            totalMessagesProcessed += 1;
        }

        public void add(TransformTombstone msg){
            // Remove from redis cache
            redisRemove(msg.key);
            // Add self to written indices
            writtenIndices.add(messages.size());
            // Update totals
            totalValueWritten += msg.value;
            tombstoneValueWritten += msg.value;
            // Add to packets and messages
            messages.add(msg);
            packets.add(msg.toJson(objectMapper));
            writtenIndices.add(messages.size());
            // Update total messages processed
            totalMessagesProcessed += 1;
        }

        public Boolean isTombstone(Integer i){
            return tombstoneIndices.contains(i);
        }

        public String getRandomKey(){
            if(redisCache.size()>2){
                return redisCache.get((int)(RandomGenerator.generateInteger(redisCache.size()-1, 0))).key;
            }
            else return "noKey";
        }

        public void redisAdd(TransformNormal msg){
            redisCache.add(msg);
            totalRedisUpdates += 1;
            totalAdded += 10.5f;
            totalValueWritten += 10.5f;
        }

        public boolean redisRemove(TransformNormal msg){
            int index = redisGetKeyIndex(msg.key);
            if (index != -1){
                redisCache.remove(index);
                totalRedisUpdates += 1;
                return true;
            }
            return false;
        }

        public boolean redisRemove(String key){
            int index = redisGetKeyIndex(key);
            if (index != -1){
                redisCache.remove(index);
                totalRedisUpdates += 1;
                return true;
            }
            return false;
        }

        public Integer redisGetKeyIndex(String key){
            for (int i = 0; i < redisCache.size(); i++){
                if (redisCache.get(i).key == key){
                    return i;
                }
            }
            return -1;
        }

        public boolean redisContainsKey(String key){
            return redisGetKeyIndex(key) != -1;
        }

        public void queueAdd(Float value){
            totalValueWritten += value;
            totalMessagesWritten += 1;
        }


    }
    public static TranMessagesData generateTransformMessages(Integer numPackets){
        //Made of
        //-tombstone packets (Contain key(String), sometimes TOTAL(Float))
        //-normal packets (Contain key(String), value(Float), version(Integer))
        // First decide what index of packets will be tombstones.
        TranMessagesData data = new TranMessagesData();
        int numTombstones = RandomGenerator.generateInteger(numPackets/4, 0);
        if (numTombstones == 0){numTombstones = 1;}

        data.tombstoneCount = numTombstones;
        data.normalCount = numPackets - numTombstones;
        data.tombstoneIndices.add(numPackets-1);// last packet tombstone to get final info.
        for (int i = 0; i < (numTombstones-1); i++){
            data.tombstoneIndices.add(RandomGenerator.generateInteger(numPackets-1,2));
        }

        // Now create the packets, generate normal packets until reach tombstoneIndex, then select one of the packets to reference the key of
        // Then generate the tombstone packet.
        for (int i = 0; i < numPackets; i++){
            if (data.isTombstone(i)){
                TransformTombstone tombstone = generateTombstonePacket(data);
                data.add(tombstone);
            } else {
                TransformNormal normal = generateNormalPacket(data);
                data.add(normal);
            }
        }
        data.calculateUnWrittenIndices();
        return data;
    }

    private static TransformNormal generateNormalPacket(TranMessagesData data){
        //Generate a normal packet
        TransformNormal normal = new TransformNormal();
        // Decide if packet reuses a key:
        Boolean reuseKey = false;
        if (data.redisCache.size() > 3){
            Float count = (float) data.messageCount();
            Float reduction = (200f / (count+200f));
            int bound = (int) ((1f - reduction)*100f);
            reuseKey = RandomGenerator.generateInteger(100,0) < bound;
        }
        if (reuseKey){
            normal.key = data.getRandomKey();
        } else {
            normal.key = RandomGenerator.generateString(5);
        }
        // Low value for easier testing
        Float value = RandomGenerator.generateFloat(20, 0);
        // Verion is 0,1,2 -  to have more conflicts while testing.
        Integer version = RandomGenerator.generateInteger(3, 0);
        normal.value = value;
        normal.version = version;
        return normal;
    }

    private static TransformTombstone generateTombstonePacket(TranMessagesData data){
        //Generate a tombstone packet
        String key = data.getRandomKey();
        TransformTombstone tombstone = new TransformTombstone();
        Boolean hasTotal = RandomGenerator.generateInteger(100,0) < 20;
        if (hasTotal){
            Float total = RandomGenerator.generateFloat(100, 0);
            tombstone.value = total;
        } else {
            tombstone.value = 0.0f;
        }
        tombstone.key = key;
        return tombstone;
    }


    /**
     * Generates a good packet JSON with a 3-4 character key
     * @return JSON string representing a good packet
     */
    private static String generateGoodPacketJson() {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        // Randomly choose between 3 or 4 characters
        int length = random.nextInt(2) + 3; // Will be either 3 or 4
        StringBuilder key = new StringBuilder();
        for (int i = 0; i < length; i++) {
            key.append(chars.charAt(random.nextInt(chars.length())));
        }
        float value = random.nextFloat() * 50;

        return String.format(
            "{\"uid\": \"s2093547\", \"key\": \"%s\", \"comment\": \" \", \"value\": %.2f}",
            key.toString(), value
        );
    }

    /**
     * Generates a bad packet JSON with a 5 character key
     * @return JSON string representing a bad packet
     */
    private static String generateBadPacketJson() {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        StringBuilder key = new StringBuilder();
        // Always generate 5 characters for bad packets
        for (int i = 0; i < 5; i++) {
            key.append(chars.charAt(random.nextInt(chars.length())));
        }
        float value = random.nextFloat() * 50;

        return String.format(
            "{\"uid\": \"s2093547\", \"key\": \"%s\", \"comment\": \" \", \"value\": %.2f}",
            key.toString(), value
        );
    }


    public static class PacketListResult {

        public String jsonList;
        public List<Float> goodTotals;
        public List<Float> badTotals;
        public List<Integer> packetTypes;
    }
    public static PacketListResult generatePacketList(int numPackets) {
        List<String> packets = new ArrayList<>();
        List<Float> goodTotals = new ArrayList<>();
        List<Float> badTotals = new ArrayList<>();
        List<Integer> packetTypes = new ArrayList<>();

        float goodRunningTotal = 0;
        float badRunningTotal = 0;

        for (int i = 0; i < numPackets; i++) {
            // Randomly decide whether to add a good or bad packet;
            String packet;
            float value;

            if (RandomGenerator.generateBoolean()) {
                packet = generateGoodPacketJson();
                // Extract value from the good packet
                value = Float.parseFloat(packet.split("\"value\": ")[1].replace("}", "").trim());
                goodRunningTotal += value;
                goodTotals.add(goodRunningTotal);
                packetTypes.add(1);
            } else {
                packet = generateBadPacketJson();
                // Extract value from the bad packet
                value = Float.parseFloat(packet.split("\"value\": ")[1].replace("}", "").trim());
                badRunningTotal += value;
                badTotals.add(badRunningTotal);
                packetTypes.add(0);
            }

            packets.add(packet);
        }

        // Create the JSON list string
        String jsonList = "[" + String.join(", ", packets) + "]";

        PacketListResult result = new PacketListResult();
        result.jsonList = jsonList;
        result.goodTotals = goodTotals;
        result.badTotals = badTotals;
        result.packetTypes = packetTypes;

        return result;
    }

    public static TransformRequest transformRequest(Integer messageCount){
        TransformRequest request = new TransformRequest();
        request.readQueue = RandomGenerator.generateRandomKey("TransformTestRead");
        request.writeQueue = RandomGenerator.generateRandomKey("TransformTestWrite");
        request.messageCount = messageCount;
        return request;
    }

    /**
     * Creates HTTP headers with Content-Type set to application/json
     */
    public static HttpHeaders createJsonHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return headers;
    }
}
