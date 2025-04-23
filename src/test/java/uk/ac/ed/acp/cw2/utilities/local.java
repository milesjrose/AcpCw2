package uk.ac.ed.acp.cw2.utilities;

import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import uk.ac.ed.acp.cw2.model.ProcessRequest;
import uk.ac.ed.acp.cw2.model.TransformMessage;
import uk.ac.ed.acp.cw2.model.TransformRequest;
import uk.ac.ed.acp.cw2.service.MongoDbService;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class local {

    private final String base;
    private final String rabbit;
    private final String kafka;
    private final String cache;
    private final TestRestTemplate restTemplate;


    public local(TestRestTemplate restTemplate, String baseUrl){
        this.base = baseUrl;
        this.kafka = base + "/kafka";
        this.rabbit = base + "/rabbitmq";
        this.cache = base + "/mongodb/cache";
        this.restTemplate = restTemplate;
    }

    public void pushRabbit(String queueName, Integer messageCount){
        ResponseEntity<Void> response = restTemplate.exchange(
                rabbit + "/" + queueName + "/" + messageCount,
                HttpMethod.PUT,
                null,
                Void.class
        );
        assertEquals(HttpStatusCode.valueOf(200), response.getStatusCode());
    }
    public List<String> recRabbit(String queueName, Integer timeoutInMsec){
        ResponseEntity<List<String>> response = restTemplate.exchange(
                rabbit + "/" + queueName + "/" + timeoutInMsec,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<String>>() {}
        );
        assertEquals(HttpStatusCode.valueOf(200), response.getStatusCode(), "Failed to receive messages from Queue");
        assertNotNull(response.getBody(), "No messages received from Queue");
        List<String> messages = response.getBody();
        assertNotNull(messages, "No messages received from Queue");
        assertFalse(messages.isEmpty(), "No messages received from Queue");

        return messages;
    }

    public ResponseEntity<Void> pushKafka(String topic, Integer count){
        return restTemplate.exchange(
                kafka + "/" + topic + "/" + count,
                HttpMethod.PUT,
                null,
                Void.class
        );
    }

    public ResponseEntity<List<String>> recKafka(String topic, Integer timeout){
        return restTemplate.exchange(
                kafka + "/" + topic + "/" + timeout,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<String>>() {}
        );
    }

    public ResponseEntity<Void> pushKafka(String topic, String json){
        return restTemplate.exchange(
                kafka + "/sendMessage/" + topic,
                HttpMethod.POST,
                new HttpEntity<>(json, PacketGenerator.createJsonHeaders()),
                Void.class
        );
    }

    public ResponseEntity<Void> procMsg(ProcessRequest request){
        return restTemplate.exchange(
                base + "/processMessages",
                HttpMethod.POST,
                new HttpEntity<>(request),
                Void.class
        );
    }

    public ResponseEntity<String> recCache(String key){
        return restTemplate.exchange(
                cache + "/" + key,
                HttpMethod.GET,
                null,
                String.class
        );
    }

    public void tranMsg(TransformRequest request){
        ResponseEntity<Void> response = restTemplate.exchange(
                base + "/transformMessages",
                HttpMethod.POST,
                new HttpEntity<>(request),
                Void.class
        );
        assertEquals(HttpStatusCode.valueOf(200), response.getStatusCode(), "Failed to transform messages");
    }

    public List<cacheEntry> clearCacheEntries(List<TransformMessage> messages, MongoDbService mongoDbService){
        List<cacheEntry> entries = new ArrayList<>();
        Set<String> keys = new HashSet<>();
        for (TransformMessage message : messages){
            String key = message.key;
            if (mongoDbService.checkKey(key)){
                keys.add(key);
            }
        }
        for (String key: keys){
            cacheEntry entry = new cacheEntry();
            entry.key = key;
            entry.entry = mongoDbService.retrieveFromCache(key);
            mongoDbService.removeFromCache(key);
        }
        return entries;
    }

}
