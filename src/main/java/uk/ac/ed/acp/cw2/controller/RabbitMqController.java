package uk.ac.ed.acp.cw2.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.acp.cw2.service.RabbitMqService;
import org.springframework.http.ResponseEntity;

import java.util.List;

/**
 * RabbitMqController is a REST controller that provides endpoints for sending and receiving stock symbols
 * through RabbitMQ. This class interacts with a RabbitMQ environment which is configured dynamically during runtime.
 */
@RestController
@RequestMapping("/rabbitmq")
public class RabbitMqController {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqController.class);
    private final RabbitMqService rabbitMqService;

    public RabbitMqController(RabbitMqService rabbitMqService) {
        this.rabbitMqService = rabbitMqService;
    }

    @PutMapping("/{queueName}/{messageCount}")
    public ResponseEntity<Void> pushToQueue(@PathVariable String queueName, @PathVariable int messageCount) {
        try {
            rabbitMqService.pushToQueue(queueName, messageCount);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            logger.error("Error pushing messages to queue", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/{queueName}/{timeoutInMsec}")
    public ResponseEntity<List<String>> receiveFromQueue(@PathVariable String queueName, @PathVariable int timeoutInMsec) {
        try {
            List<String> messages = rabbitMqService.receiveFromQueue(queueName, timeoutInMsec);
            return ResponseEntity.ok(messages);
        } catch (Exception e) {
            logger.error("Error receiving messages from queue", e);
            return ResponseEntity.internalServerError().build();
        }
    }
}