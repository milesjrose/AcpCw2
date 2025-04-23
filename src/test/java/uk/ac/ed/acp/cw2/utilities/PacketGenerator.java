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


public class PacketGenerator {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static Random random = new Random();

    public static class TransformData{
        

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

        List<String> keys;
        List<Integer> versions;
        List<Integer> tombstoneIndices;
        @Getter
        List<TransformMessage> messages;
        @Getter
        List<ObjectNode> packets;
        public Integer messageCount(){
            return tombstoneCount + normalCount;
        }

        public TransformData(){
            totalValueWritten = 0f;
            tombstoneCount = 0;
            normalCount = 0;
            totalRedisUpdates = 0;
            totalMessagesWritten = 0;
            totalAdded = 0f;
            totalMessagesProcessed = 0;
            keys = new ArrayList<>();
            versions = new ArrayList<>();
            tombstoneIndices = new ArrayList<>();
            messages = new ArrayList<>();
            packets = new ArrayList<>();
        }

        public void add(TransformNormal msg){
            if (keys.contains(msg.key)){
                int index = keys.indexOf(msg.key);
                if (msg.version>versions.get(index)){
                    redisAdd();
                }
            } else {
                redisAdd();
            }
            queueAdd(msg.value);
            messages.add(msg);
            keys.add(msg.key);
            versions.add(msg.version);
            packets.add(msg.toJson(objectMapper));
            totalMessagesProcessed += 1;
        }

        public void add(TransformTombstone msg){
            int index = keys.indexOf(msg.key);
            keys.remove(msg.key);
            versions.remove(index);
            totalValueWritten += msg.value;
            packets.add(msg.toJson(objectMapper));
            messages.add(msg);
            totalRedisUpdates += 1;
            totalMessagesProcessed += 1;
        }

        public Boolean isTombstone(Integer i){
            return tombstoneIndices.contains(i);
        }

        public String getRandomKey(){
            if(keys.size()>0){
                return keys.get(RandomGenerator.generateInteger(keys.size()-1, 0));
            }
            else return "noKey";
        }

        public void redisAdd(){
            totalRedisUpdates += 1;
            totalAdded += 10.5f;
            totalValueWritten += 10.5f;
        }

        public void queueAdd(Float value){
            totalValueWritten += value;
            totalMessagesWritten += 1;
        }


    }
    public static TransformData generateTransformMessage(Integer numPackets){
        //Made of
        //-tombstone packets (Contain key(String), sometimes TOTAL(Float))
        //-normal packets (Contain key(String), value(Float), version(Integer))
        // First decide what index of packets will be tombstones.
        TransformData data = new TransformData();
        int numTombstones = RandomGenerator.generateInteger(numPackets/4, 0);
        if (numTombstones == 0){numTombstones = 1;}

        data.tombstoneCount = numTombstones;
        data.normalCount = numPackets - numTombstones;
        for (int i = 0; i < numTombstones; i++){
            data.tombstoneIndices.add(RandomGenerator.generateInteger(numPackets-1,2));
        }

        // Now create the packets, generate normal packets until reach tombstoneIndex, then select one of the packets to reference the key of
        // Then generate the tombstone packet.
        for (int i = 0; i < numPackets; i++){
            if (data.isTombstone(i)){
                String key = data.getRandomKey();
                TransformTombstone tombstone = generateTombstonePacket(key);
                data.add(tombstone);
            } else {
                TransformNormal normal = generateNormalPacket();
                data.add(normal);
            }
        }
        return data;
    }

    private static TransformNormal generateNormalPacket(){
        //Generate a normal packet
        TransformNormal normal = new TransformNormal();
        String key = RandomGenerator.generateString(5);
        Float value = RandomGenerator.generateFloat(100, 0);
        Integer version = RandomGenerator.generateInteger(100, 0);
        normal.key = key;
        normal.value = value;
        normal.version = version;
        return normal;
    }

    private static TransformTombstone generateTombstonePacket(String key){
        //Generate a tombstone packet
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

    public static TransformRequest transformRequest(){
        TransformRequest request = new TransformRequest();
        request.readQueue = RandomGenerator.generateRandomKey("TransformTestRead");
        request.writeQueue = RandomGenerator.generateRandomKey("TransformTestWrite");
        request.messageCount = 10;
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
