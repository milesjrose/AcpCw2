package uk.ac.ed.acp.cw2.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;
import uk.ac.ed.acp.cw2.domain.ProcMessage;
import uk.ac.ed.acp.cw2.model.ProcessRequest;

import java.util.ArrayList;
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

    public void processMessages(ProcessRequest request) {
        logger.info("Processing messages; topic:{}, good_queue:{}, bad_queue:{}, count:{}",
            request.readTopic, request.writeQueueGood, request.writeQueueBad, request.messageCount);

        int offset = 0;
        List<ProcMessage> messages = new ArrayList<>();

        // Get messages
        while (messages.size() < request.messageCount){
            // Get JSON strings from topic
            List<String> messageStrings = kafkaService.receiveFromTopic(request.readTopic, 5000, request.messageCount+offset);
            // Remove any already proccessed items
            messageStrings.subList(0, Math.min(offset, messageStrings.size())).clear();
            // Update offset
            offset += messageStrings.size();
            // Convert strings to messages
            for (String messageString : messageStrings){
                try {
                    ProcMessage message = new ProcMessage(messageString);
                    messages.add(message);
                } catch (Exception ignored) {}
            }
            logger.info("Received {}/{} valid messages", messages.size(), request.messageCount);
        }

        // Proccess messages
        MessageProcessor messageProcessor = new MessageProcessor(request,  mongoDbService, rabbitMqService);
        messageProcessor.proccessMessages(messages);
    }

    public void transformMessages(TransformRequest request){
        logger.info("Transforming messages; read_queue:{}, write_queue:{}, count:{}",
            request.readQueue, request.writeQueue, request.messageCount);

        List<String> messageStrings = rabbitMqService.receiveFromQueue(request.readQueue, 0);
        List<TransformMessage> messages = new ArrayList<>();
        for (String messageString : messageStrings){
            try {
                messages.add(new TransformMessage(messageString));
            } catch (Exception ignored) {}
        }
        
    }
}
