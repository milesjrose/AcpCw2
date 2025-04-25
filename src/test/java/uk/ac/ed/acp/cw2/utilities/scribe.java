package uk.ac.ed.acp.cw2.utilities;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import uk.ac.ed.acp.cw2.model.ProcessRequest;
import uk.ac.ed.acp.cw2.model.TransformMessage;
import uk.ac.ed.acp.cw2.domain.MessageTransformer;
import uk.ac.ed.acp.cw2.utilities.PacketGenerator.TranMessagesData;

public class Scribe {
    private static final Logger logger = LoggerFactory.getLogger(Scribe.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void logTest(String test, String message){
        String filename = Paths.get("logs", "test", test + ".log").toString();
        try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
            writer.println(String.format("=== %s ===", test));
            writer.println(message);
        } catch (IOException e) {
            logger.error("Failed to write test data to log file", e);
        }
    }

    public static void logProc(ProcessRequest request, String json, List<String> messages, List<String> badMessages, PacketGenerator.PacketListResult packetList){
        String filename = Paths.get("logs", "proc", request.readTopic + ".log").toString();
        try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
            writer.println("=== SERVICE TEST DATA ===");
            writer.println("\n=== SENT JSON ===");
            writer.println(json);

            writer.println("\n=== GOOD PACKET TOTALS ===");
            for (int i = 0; i < packetList.goodTotals.size(); i++) {
                writer.printf("Index %d: %.2f%n", i, packetList.goodTotals.get(i));
            }

            writer.println("\n=== BAD PACKET TOTALS ===");
            for (int i = 0; i < packetList.badTotals.size(); i++) {
                writer.printf("Index %d: %.2f%n", i, packetList.badTotals.get(i));
            }

            writer.println("\n=== GOOD QUEUE MESSAGES ===");
            for (String message : messages) {
                writer.println(message);
            }

            writer.println("\n=== BAD QUEUE MESSAGES ===");
            for (String message : badMessages) {
                writer.println(message);
            }
            writer.println("--------------------------------");
        } catch (IOException e) {
            logger.error("Failed to write test data to log file", e);
        }
    }

    public static void logTransform(TranTestData calledRunData, TranTestData manualReceivedData, MessageTransformer transformer, TranMessagesData generatedData) {
        String timestamp = LocalDateTime.now().toString().replace(':', '-');
        String filename = Paths.get("logs", "tran", timestamp + ".log").toString();
        try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
            writer.println("=== TRANSFORM TEST DATA ===");
            // General data
            writer.println("\n> Generation Data");
            writer.println("    numPackets: " + generatedData.messageCount());
            writer.println("    Total Messages Written: " + generatedData.getTotalMessagesWritten());
            writer.println("    Total Messages Processed: " + generatedData.getTotalMessagesProcessed());
            writer.println("    Total Redis Updates: " + generatedData.getTotalRedisUpdates());
            writer.println("    Total Value Written: " + generatedData.getTotalValueWritten());
            writer.println("    Total Added: " + generatedData.getTotalAdded());

            writer.println("\n> Redis cache data:");
            int redisSize = calledRunData.entries.size();
            writeDifferingData(writer, "REDIS SIZE", redisSize, manualReceivedData.entries.size());
            writer.println("    Redis Size: " + redisSize);

            int redisUpdates = generatedData.getTotalRedisUpdates();
            writeDifferingData(writer, "REDIS DELTA", redisUpdates, calledRunData.totalRedisUpdates);
            writer.println("    Redis Delta: " + (redisUpdates-redisSize));

            writer.println("\n> Message data");
            writer.println("    Tombstone Value Written: " + generatedData.getTombstoneValueWritten());
            writer.println("    Normal Count: " + generatedData.getNormalCount());
            writer.println("    Tombstone Count: " + generatedData.getTombstoneCount());
            writer.println("    Tombstone Indices: " + generatedData.getTombstoneIndices());
            writer.println("    Unwritten Count: " + (generatedData.messageCount() - generatedData.getTotalMessagesWritten()));
            writer.println("    Unwritten Indices: " + generatedData.getUnWrittenIndices());
            writer.println("    Unstored Count: " + generatedData.getUnstoredCount());
            writer.println("    Unstored Indices: " + generatedData.getUnstoredIndices());
            writer.println("    Removed Count: " + generatedData.getRemovedMessages().size());
            writer.println("    Removed Message Indices: " + generatedData.getRemovedMessageIndices());
            
            writeDifferingDatas(writer, calledRunData, manualReceivedData, generatedData);

            // First run data
            writer.println("\n=== CALLED RUN DATA ===");
            writer.println("Request: " + objectMapper.writeValueAsString(calledRunData.request));
            writer.println("\n> Stats");
            writer.println("    Total Messages Written: " + calledRunData.totalMessagesWritten);
            writer.println("    Total Messages Processed: " + calledRunData.totalMessagesProcessed);
            writer.println("    Total Redis Updates: " + calledRunData.totalRedisUpdates);
            writer.println("    Total Value Written: " + calledRunData.totalValueWritten);
            writer.println("    Total Added: " + calledRunData.totalAdded);
            // messages
            writer.println("\n> Written Messages:" + calledRunData.messages.size());
            wnStrings(calledRunData.messages, writer);
            writer.println("\n> Cache Entries:" + calledRunData.entries.size());
            wnStrings(calledRunData.entries, writer);

            // Second run data
            writer.println("\n=== MANUAL RUN DATA ===");
            writer.println("Request: " + objectMapper.writeValueAsString(manualReceivedData.request));
            writer.println("\n> Stats");
            writer.println("    Total Messages Written: " + manualReceivedData.totalMessagesWritten);
            writeDifferingData(writer, "Total Messages Written", manualReceivedData.totalMessagesWritten, transformer.getTotalMessagesWritten());
            writer.println("    Total Messages Processed: " + manualReceivedData.totalMessagesProcessed);
            writeDifferingData(writer, "Total Messages Processed", manualReceivedData.totalMessagesProcessed, transformer.getTotalMessagesProcessed());
            writer.println("    Total Redis Updates: " + manualReceivedData.totalRedisUpdates);
            writeDifferingData(writer, "Total Redis Updates", manualReceivedData.totalRedisUpdates, transformer.getTotalRedisUpdates());
            writer.println("    Total Value Written: " + manualReceivedData.totalValueWritten);
            writeDifferingData(writer, "Total Value Written", manualReceivedData.totalValueWritten, transformer.getTotalValueWritten());
            writer.println("    Total Added: " + manualReceivedData.totalAdded);
            writeDifferingData(writer, "Total Added", manualReceivedData.totalAdded, transformer.getTotalAdded());

            writer.println("\n> Written Messages:" + manualReceivedData.messages.size());
            wnStrings(manualReceivedData.messages, writer);
            writer.println("\n> Cache Entries:" + manualReceivedData.entries.size());
            wnStrings(manualReceivedData.entries, writer);
            writer.println("\n> Read Messages (with updated values):" + transformer.getMessages().size());
            wnTranMsgs(transformer.getMessages(), writer);

            // Generated data
            writer.println("\n=== GENERATED DATA ===");
            writer.println("\n> Stats");
            writer.println("    Total Messages Written: " + generatedData.getTotalMessagesWritten());
            writer.println("    Total Messages Processed: " + generatedData.getTotalMessagesProcessed());
            writer.println("    Total Redis Updates: " + generatedData.getTotalRedisUpdates());
            writer.println("    Total Value Written: " + generatedData.getTotalValueWritten());
            writer.println("    Total Added: " + generatedData.getTotalAdded());
            writer.println("\n> Generation Data:");
            writer.println("    Tombstone Value Written: " + generatedData.getTombstoneValueWritten());
            writer.println("    Tombstone Count: " + generatedData.getTombstoneCount());
            writer.println("    Normal Count: " + generatedData.getNormalCount());
            writer.println("    Unstored Count: " + generatedData.getUnstoredCount());
            writer.println("    Removed Count: " + generatedData.getRemovedMessages().size());
            writer.println("    Removed Message Indices: " + generatedData.getRemovedMessageIndices());

            // Removed messages (with indices)
            Integer removed = generatedData.getRemovedMessages().size();
            writer.println("\n> Removed Messages:" + removed);
            if (removed > 0 && removed == generatedData.getRemovedMessageIndices().size()){
                wnTranMsgs(generatedData.getRemovedMessages(), generatedData.getRemovedMessageIndices(), writer);
            } else if (removed > 0){
                writer.println("!DIFFERENT REMOVED MESSAGE INDICES SIZE: " + removed + ", " + generatedData.getRemovedMessageIndices().size());
                wnTranMsgs(generatedData.getRemovedMessages(), writer);
            }

            // Unstored messages (with indices)
            Integer unstored = generatedData.getUnstoredMessages().size();
            writer.println("\n> Unstored Messages:" + unstored);
            if (unstored > 0 && unstored == generatedData.getUnstoredIndices().size()){
                wnTranMsgs(generatedData.getUnstoredMessages(), generatedData.getUnstoredIndices(), writer);
            } else if (unstored > 0){
                writer.println("    !DIFFERENT UNSTORED MESSAGE INDICES SIZE: " + unstored + ", " + generatedData.getUnstoredIndices().size());
                wnTranMsgs(generatedData.getUnstoredMessages(), writer);
            }

            // Generated messages
            writer.println("\n> Generated Messages");
            wnTranMsgs(generatedData.getMessages(), writer);

            // Generated packet
            writer.println("\n> Generated Packets");
            wnObjNodes(generatedData.getPackets(), writer);
        } catch (IOException e) {
            logger.error("Failed to write transform test data to log file", e);
        }
    }

    private static void wnStrings(List<String> messages, PrintWriter writer){
        for (int i = 0; i < messages.size(); i++){
            writer.println("    [" + i + "]: " + messages.get(i));
        }
    }

    private static void wnTranMsgs(List<TransformMessage> messages, PrintWriter writer){
        for (int i = 0; i < messages.size(); i++){
            writer.println("    [" + i + "]: " + messages.get(i).toJson(objectMapper));
        }
    }

    private static void wnObjNodes(List<ObjectNode> messages, PrintWriter writer){
        for (int i = 0; i < messages.size(); i++){
            writer.println("    [" + i + "]: " + messages.get(i).toString());
        }
    }

    private static void wnStrings(List<String> messages, List<Integer> indices, PrintWriter writer){
        for (int i = 0; i < indices.size(); i++){
            writer.println("    [" + indices.get(i) + "]: " + messages.get(i));
        }
    }

    private static void wnTranMsgs(List<TransformMessage> messages, List<Integer> indices, PrintWriter writer){
        for (int i = 0; i < indices.size(); i++){
            writer.println("    [" + indices.get(i) + "]: " + messages.get(i).toJson(objectMapper));
        }
    }

    private static void wnObjNodes(List<ObjectNode> messages, List<Integer> indices, PrintWriter writer){
        for (int i = 0; i < indices.size(); i++){
            writer.println("    [" + indices.get(i) + "]: " + messages.get(i).toString());
        }
    }

    private static void writeDifferingDatas(PrintWriter writer, TranTestData calledRunData, TranTestData manualReceivedData, TranMessagesData generatedData){
        writer.println("\nDiffering Data: (Called Run, Manual Run, Generated Data)");
        // Stats
        writeDifferingData(writer, "Total Messages Written", calledRunData.totalMessagesWritten, manualReceivedData.totalMessagesWritten, generatedData.getTotalMessagesWritten());
        writeDifferingData(writer, "Total Messages Processed", calledRunData.totalMessagesProcessed, manualReceivedData.totalMessagesProcessed, generatedData.getTotalMessagesProcessed());
        writeDifferingData(writer, "Total Redis Updates", calledRunData.totalRedisUpdates, manualReceivedData.totalRedisUpdates, generatedData.getTotalRedisUpdates());
        writeDifferingData(writer, "Total Value Written", calledRunData.totalValueWritten, manualReceivedData.totalValueWritten, generatedData.getTotalValueWritten());
        writeDifferingData(writer, "Total Added", calledRunData.totalAdded, manualReceivedData.totalAdded, generatedData.getTotalAdded());
        // Stored/written data info
        writeDifferingData(writer, "Redis Size", calledRunData.entries.size(), manualReceivedData.entries.size(), generatedData.getTotalRedisUpdates());
        writeDifferingData(writer, "Redis Delta", calledRunData.totalRedisUpdates - calledRunData.entries.size(), manualReceivedData.totalRedisUpdates - manualReceivedData.entries.size(), generatedData.getTotalRedisUpdates() - calledRunData.entries.size());
        writeDifferingData(writer, "Written Messages", calledRunData.messages.size(), manualReceivedData.messages.size(), generatedData.getTotalMessagesWritten());
        writer.println("--------------------------------");
        // Written data per run, e.g. messages.size == totalMessagesWritten
        writeDifferingData(writer, "messagesSize/totalMessagesWritten (Called Run)", calledRunData.messages.size(), calledRunData.totalMessagesWritten);
        writeDifferingData(writer, "messagesSize/totalMessagesWritten (Manual Run)", manualReceivedData.messages.size(), manualReceivedData.totalMessagesWritten);
        writer.println("--------------------------------");
    }

    private static void writeDifferingData(PrintWriter writer, String name, Integer val1, Integer val2, Integer val3){
        if (val1 != val2 && val1 != val3){
            writer.println("    !DIFFERENT "+name+": " + val1 + ", " + val2 + ", " + val3);
        }
    }

    private static void writeDifferingData(PrintWriter writer, String name, Integer val1, Integer val2){
        if (val1 != val2){
            writer.println("    !DIFFERENT "+name+": " + val1 + ", " + val2);
        }
    }

    private static void writeDifferingData(PrintWriter writer, String name, Float val1, Float val2, Float val3){
        if (val1 != val2 && val1 != val3){
            writer.println("    !DIFFERENT "+name+": " + val1 + ", " + val2 + ", " + val3);
        }
    }

    private static void writeDifferingData(PrintWriter writer, String name, Float val1, Float val2){
        if (val1 != val2){
            writer.println("    !DIFFERENT "+name+": " + val1 + ", " + val2);
        }
    }

}
