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
            boolean success = rabbitMqService.pushToQueue(queueName, messageCount);
            return success ? ResponseEntity.ok().build() : ResponseEntity.internalServerError().build();
        } catch (Exception e) {
            logger.error("Uncaught error pushing messages to queue", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/{queueName}/{timeoutInMsec}")
    public ResponseEntity<List<String>> receiveFromQueue(@PathVariable String queueName, @PathVariable int timeoutInMsec) {
        try {
            List<String> messages = rabbitMqService.receiveTimeout(queueName, timeoutInMsec);
            return messages != null ? ResponseEntity.ok(messages) : ResponseEntity.internalServerError().build();
        } catch (Exception e) {
            logger.error("Uncaught error receiving messages from queue", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/{queueName}/count")
    public ResponseEntity<Integer> getQueueMessageCount(@PathVariable String queueName) {
        try {
            long count = rabbitMqService.getQueueMessageCount(queueName);
            return count != -1 ? ResponseEntity.ok((int)count) : ResponseEntity.internalServerError().build();
        } catch (Exception e) {
            logger.error("Uncaught error getting queue message count", e);
            return ResponseEntity.internalServerError().build();
        }
    }
}