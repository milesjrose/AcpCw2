package uk.ac.ed.acp.cw2.service;

import uk.ac.ed.acp.cw2.Utilities.Parser;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ed.acp.cw2.model.BlobPacket;
import uk.ac.ed.acp.cw2.Utilities.Headers;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Service
public class StorageService {

    private static final Logger logger = LoggerFactory.getLogger(StorageService.class);
    private final String pushUrl;
    private final String recUrl;
    private final String delUrl;
    private final String defualtStoreName = "store-s2093547";
    private final RestTemplate restTemplate;

    public StorageService(RuntimeEnvironment environment) {
        this.restTemplate = new RestTemplate();
        String storageServiceUrl = environment.getStorageServiceUrl();
        // Ensure URL has protocol
        if (!storageServiceUrl.startsWith("http://") && !storageServiceUrl.startsWith("https://")) {
            storageServiceUrl = "https://" + storageServiceUrl;
        }
        this.pushUrl = storageServiceUrl + "/api/v1/data_definition/blob";
        this.recUrl = storageServiceUrl + "/api/v1/blob/";
        this.delUrl = storageServiceUrl + "/api/v1/blob/";
    }

    public BlobPacket pushBlob(BlobPacket packet) {
        logger.info("Pushing blob to storage service: {}", packet.pushJson());
        HttpEntity<String> request = new HttpEntity<>(packet.pushJson(), Headers.createJsonHeaders());
        try{
            ResponseEntity<String> response = restTemplate.postForEntity(pushUrl, request, String.class);
            if (response.getStatusCode().equals(HttpStatus.valueOf(200))) {
                packet.uuid = Parser.parseString(response.getBody());
                logger.info("Uploaded string to storage service: {} ({})", response.getBody(), packet.uuid);
                return packet;
            } else {
                    throw new Exception(String.format("Bad response: %s", response.getStatusCode()));
            }
        } catch (Exception e) {
            logger.error("Failed to push blob to storage service: {}", e.getMessage());
            return null;
        } 
    }

    public BlobPacket receiveBlob(BlobPacket packet) {
        logger.info("Receiving blob from storage service: {}", packet.uuid);
        if (packet.uuid == null) {
            logger.error("UUID is null");
            return null;
        }
        String url = recUrl + packet.uuid;
        try{
            ResponseEntity<BlobPacket> response = restTemplate.exchange(
                url,
                HttpMethod.GET,
                null,
                BlobPacket.class
            );

            if (response.getStatusCode().equals(HttpStatus.valueOf(200))) {
                BlobPacket receivedPacket = response.getBody();
                if (receivedPacket != null) {
                    packet.datasetName = receivedPacket.datasetName;
                    packet.data = receivedPacket.data;
                    logger.info("Received blob from storage service: {} ({}, {})", response.getBody(), packet.datasetName, packet.data);
                    return packet;
                } else {
                    throw new Exception(String.format("Null blob: %s", response.getStatusCode()));
                }
            } else {
                throw new Exception(String.format("Bad response: %s", response.getStatusCode()));
            }
        } catch (Exception e) {
            logger.error("Failed to receive blob from storage service: {} {}", url, e.getMessage());
            return null;
        }
    }

    public boolean deleteBlob(String uuid) {
        if (uuid == null) {
            logger.error("UUID is null");
            return false;
        }

        String url = delUrl + uuid;
        try{
            ResponseEntity<Void> response = restTemplate.exchange(
                url,
                HttpMethod.DELETE,
                null,
                Void.class
            );

            if (response.getStatusCode().equals(HttpStatus.valueOf(200))) {
                logger.info("Deleted blob from storage service: {}", uuid);
                return true;
            } else {
                throw new Exception(String.format("Bad response: %s", response.getStatusCode()));
            }
        } catch (Exception e) {
            logger.error("Failed to delete blob from storage service: {} {}", url, e.getMessage());
            return false;
        }
    }

    public String pushBlob(String datasetName, String data) {
        BlobPacket packet = new BlobPacket(datasetName, data);
        return pushBlob(packet).uuid;
    }

    public BlobPacket receiveBlob(String uuid) {
        BlobPacket packet = new BlobPacket(uuid);
        return receiveBlob(packet);
    }

    public boolean deleteBlob(BlobPacket packet) {
        return deleteBlob(packet.uuid);
    }

    public String pushBlob(ObjectNode node) {
        return pushBlob(defualtStoreName, node.toString().replace("\"", "\\\""));
    }

} 