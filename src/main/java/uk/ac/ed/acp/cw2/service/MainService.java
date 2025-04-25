package uk.ac.ed.acp.cw2.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;
import uk.ac.ed.acp.cw2.domain.MessageProcessor;
import uk.ac.ed.acp.cw2.domain.MessageTransformer;
import uk.ac.ed.acp.cw2.model.ProcessRequest;
import uk.ac.ed.acp.cw2.model.TransformRequest;

@Service
public class MainService {
    private final RabbitMqService rabbitMqService;
    private final KafkaService kafkaService;
    private final StorageService storageService;
    private final CacheService cacheService;

    @Autowired
    public MainService(RuntimeEnvironment environment,
                       RabbitMqService rabbitMqService,
                       CacheService cacheService,
                       KafkaService kafkaService,
                       StorageService storageService) {
        this.rabbitMqService = rabbitMqService;
        this.cacheService = cacheService;
        this.kafkaService = kafkaService;
        this.storageService = storageService;
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
        MessageProcessor messageProcessor = new MessageProcessor(request, storageService, rabbitMqService, kafkaService);
        messageProcessor.proccessMessages();
    }

    public void transformMessages(TransformRequest request){
        MessageTransformer transformer = new MessageTransformer(request, cacheService, rabbitMqService);
        transformer.transformMessages();
    }
}
