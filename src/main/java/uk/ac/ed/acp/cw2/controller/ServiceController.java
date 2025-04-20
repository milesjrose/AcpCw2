package uk.ac.ed.acp.cw2.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.acp.cw2.service.MainService;
import uk.ac.ed.acp.cw2.model.MessageRequest;

/**
 * Controller class that handles various HTTP endpoints for the application.
 * Provides functionality for serving the index page, retrieving a static UUID,
 * and managing key-value pairs through POST requests.
 */
@RestController
@RequestMapping("/api/v1")
public class ServiceController {

    private static final Logger logger = LoggerFactory.getLogger(ServiceController.class);
    private final MainService mainService;

    public ServiceController(MainService mainService) {
        this.mainService = mainService;
    }

    @GetMapping("/")
    public String index() {
        return mainService.getIndexPage();
    }

    @GetMapping("/uuid")
    public String uuid() {
        return mainService.getUuid();
    }

    @PostMapping("/processMessages")
    public ResponseEntity<Void> processMessages(@RequestBody MessageRequest request) {
        try {
            mainService.processMessages(request);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            logger.error("Error processing messages", e);
            return ResponseEntity.internalServerError().build();
        }
    }
}
