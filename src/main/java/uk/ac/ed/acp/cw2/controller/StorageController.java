package uk.ac.ed.acp.cw2.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.acp.cw2.service.StorageService;
import uk.ac.ed.acp.cw2.model.BlobPacket;
import org.springframework.http.HttpStatus;

@RestController
@RequestMapping("/api/storage")
public class StorageController {

    private final StorageService storageService;

    public StorageController(StorageService storageService) {
        this.storageService = storageService;
    }

    @PostMapping("/pushBlob/{dataSetName}/{data}")
    public ResponseEntity<String> pushBlob(@PathVariable String dataSetName, @PathVariable String data) {
        BlobPacket packet = new BlobPacket(dataSetName, data);
        BlobPacket uploadedPacket = storageService.pushBlob(packet);
        if (uploadedPacket == null) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
        return ResponseEntity.ok(uploadedPacket.uuid);
    }

    @GetMapping("/receiveBlob/{uuid}")
    public ResponseEntity<String> receiveBlob(@PathVariable String uuid) {
        BlobPacket packet = storageService.receiveBlob(uuid);
        if (packet == null) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(packet.receiveJson());
    }

    @DeleteMapping("/delete/{uuid}")
    public ResponseEntity<Void> deleteBlob(@PathVariable String uuid) {
        boolean deleted = storageService.deleteBlob(uuid);
        if (deleted) {
            return ResponseEntity.ok().build();
        } else {
            return ResponseEntity.notFound().build();
        }
    }
} 