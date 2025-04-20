package uk.ac.ed.acp.cw2.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.acp.cw2.model.Message;
import uk.ac.ed.acp.cw2.service.KafkaService;
import org.springframework.http.ResponseEntity;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;

/**
 * KafkaController is a REST API controller used to interact with Apache Kafka for producing
 * and consuming stock symbol events. This class provides endpoints for sending stock symbols
 * to a Kafka topic and retrieving stock symbols from a Kafka topic.
 */
@RestController
@RequestMapping("/kafka")
public class KafkaController {

    private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);
    private final KafkaService kafkaService;

    public KafkaController(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
    }

    @PostMapping("/sendStockSymbols/{symbolTopic}/{symbolCount}")
    public void sendStockSymbols(@PathVariable String symbolTopic, @PathVariable int symbolCount) {
        kafkaService.sendStockSymbols(symbolTopic, symbolCount);
    }

    @GetMapping("/receiveStockSymbols/{symbolTopic}/{consumeTimeMsec}")
    public List<AbstractMap.SimpleEntry<String, String>> receiveStockSymbols(@PathVariable String symbolTopic, @PathVariable int consumeTimeMsec) {
        return kafkaService.receiveStockSymbols(symbolTopic, consumeTimeMsec);
    }

    @PutMapping("/{writeTopic}/{messageCount}")
    public ResponseEntity<Void> pushToTopic(@PathVariable String writeTopic, @PathVariable int messageCount) {
        try {
            boolean success = kafkaService.pushToTopic(writeTopic, messageCount);
            return success ? ResponseEntity.ok().build() : ResponseEntity.internalServerError().build();
        } catch (Exception e) {
            logger.error("Error pushing messages to Kafka topic", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/{readTopic}/{timeoutInMsec}")
    public ResponseEntity<List<String>> receiveFromTopic(@PathVariable String readTopic, @PathVariable int timeoutInMsec) {
        try {
            List<String> messages = kafkaService.receiveFromTopic(readTopic, timeoutInMsec);
            return messages != null ? ResponseEntity.ok(messages) : ResponseEntity.internalServerError().build();
        } catch (Exception e) {
            logger.error("Error receiving messages from Kafka topic", e);
            return ResponseEntity.internalServerError().build();
        }
    }

    @PostMapping("/sendMessage/{topic}")
    public ResponseEntity<Void> sendMessage(@PathVariable String topic, @RequestBody List<Message> messages) {
        List<String> messageStrings = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        for (Message message : messages){
            messageStrings.add(message.toString(objectMapper));
        }
        kafkaService.send(topic, messageStrings);
        return ResponseEntity.ok().build();
    }
}
