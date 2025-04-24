package uk.ac.ed.acp.cw2.utilities;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import uk.ac.ed.acp.cw2.model.ProcessRequest;
import uk.ac.ed.acp.cw2.model.TransformRequest;
import uk.ac.ed.acp.cw2.model.TransformMessage;
import uk.ac.ed.acp.cw2.model.TransformNormal;
import uk.ac.ed.acp.cw2.model.TransformTombstone;
import uk.ac.ed.acp.cw2.service.MessageTransformer;
import uk.ac.ed.acp.cw2.utilities.PacketGenerator.TranMessagesData;

public class scribe {
    private static final Logger logger = LoggerFactory.getLogger(scribe.class);
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
        String filename = Paths.get("logs", "tran", calledRunData.request.writeQueue + ".log").toString();
        try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
            writer.println("=== TRANSFORM TEST DATA ===");
            // General data
            writer.println("\n=== GENERAL DATA ===");
            writer.println("indices: " + generatedData.getTombstoneIndices());
            writer.println("numPackets: " + generatedData.messageCount());
            writer.println("Normal Count: " + generatedData.getNormalCount());
            writer.println("Tombstone Count: " + generatedData.getTombstoneCount());
            
            // First run data
            writer.println("\n=== FIRST RUN DATA ===");
            writer.println("Request: " + objectMapper.writeValueAsString(calledRunData.request));
            writer.println("\nMessages:");
            for (String message : calledRunData.messages) {
                writer.println(message);
            }
            writer.println("\nCache Entries:");
            for (cacheEntry entry : calledRunData.entries) {
                writer.println(entry.toString());
            }

            // Second run data
            writer.println("\n=== SECOND RUN DATA ===");
            writer.println("Request: " + objectMapper.writeValueAsString(manualReceivedData.request));
            writer.println("\nMessages:");
            for (String message : manualReceivedData.messages) {
                writer.println(message);
            }
            writer.println("\nCache Entries:");
            for (cacheEntry entry : manualReceivedData.entries) {
                writer.println(entry.toString());
            }

            // Transformer statistics
            writer.println("\n=== TRANSFORMER STATISTICS ===");
            writer.println("Total Messages Written: " + transformer.getTotalMessagesWritten());
            writer.println("Total Messages Processed: " + transformer.getTotalMessagesProcessed());
            writer.println("Total Redis Updates: " + transformer.getTotalRedisUpdates());
            writer.println("Total Value Written: " + transformer.getTotalValueWritten());
            writer.println("Total Added: " + transformer.getTotalAdded());

            // Generated data
            writer.println("\n=== GENERATED DATA ===");
            writer.println("Total Messages Written: " + generatedData.getTotalMessagesWritten());
            writer.println("Total Messages Processed: " + generatedData.getTotalMessagesProcessed());
            writer.println("Total Redis Updates: " + generatedData.getTotalRedisUpdates());
            writer.println("Total Value Written: " + generatedData.getTotalValueWritten());
            writer.println("Total Added: " + generatedData.getTotalAdded());

            // Generated messages
            writer.println("\nGenerated Messages:");
            for (TransformMessage message : generatedData.getMessages()) {
                writer.println(message.toJson(objectMapper));
            }

            // Generated packets
            
            writer.println("\nGenerated Packets:");
            for (ObjectNode packet : generatedData.getPackets()) {
                writer.println(packet.toString());
            }
        } catch (IOException e) {
            logger.error("Failed to write transform test data to log file", e);
        }
    }
}
