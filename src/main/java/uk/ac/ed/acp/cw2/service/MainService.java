package uk.ac.ed.acp.cw2.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;
import uk.ac.ed.acp.cw2.model.Message;
import uk.ac.ed.acp.cw2.model.MessageRequest;

import java.util.List;

@Service
public class MainService {
    private static final Logger logger = LoggerFactory.getLogger(MainService.class);
    private final RuntimeEnvironment environment;
    private final RabbitMqService rabbitMqService;
    private final MongoDbService mongoDbService;
    private final KafkaService kafkaService;

    @Autowired
    public MainService(RuntimeEnvironment environment,
                       RabbitMqService rabbitMqService,
                       MongoDbService mongoDbService,
                       KafkaService kafkaService) {
        this.environment = environment;
        this.rabbitMqService = rabbitMqService;
        this.mongoDbService = mongoDbService;
        this.kafkaService = kafkaService;
    }

    public String getIndexPage() {
        StringBuilder currentEnv = new StringBuilder();
        currentEnv.append("<ul>");
        System.getenv().keySet().forEach(key -> currentEnv.append("<li>").append(key).append(" = ").append(System.getenv(key)).append("</li>"));
        currentEnv.append("</ul>");

        return "<html><body>" +
                "<h1>Welcome from ACP CW2</h1>" +
                "<h2>Environment variables </br><div> " + currentEnv.toString() + "</div></h2>" +
                "</body></html>";
    }

    public String getUuid() {
        return "s2093547";
    }

    public void processMessages(MessageRequest request) {
        logger.info("Processing messages from topic: {}, writing to good queue: {}, writing to bad queue: {}, with message count: {}", 
            request);
        
        MessageProccessor messageProccessor = new MessageProccessor(request,  mongoDbService, rabbitMqService);
        
        int remainingMessages = request.messageCount;

        // Process messages until there are no more messages to process
        while (remainingMessages > 0) {
            // Get messages from the topic, if there are no messages left, just return the received messages
            List<Message> messages = kafkaService.receiveMessages(request.readTopic, 5000, remainingMessages);;
            remainingMessages -= messages.size();
            messageProccessor.addMessages(messages);    // Add messages to the message processor.
            messageProccessor.checkMessages();          // Check if the messages are good or bad, and add totals.
            messageProccessor.processMessages();        // Store and queue good and bad messages.
        }
        messageProccessor.sendTotalValues(); // Send totals to queues.
    }
}
