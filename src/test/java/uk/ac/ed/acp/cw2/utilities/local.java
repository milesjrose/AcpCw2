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
import uk.ac.ed.acp.cw2.service.CacheService;
import uk.ac.ed.acp.cw2.Utilities.Parser;

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

    public void pushKafka(String topic, Integer count){
        ResponseEntity<Void> response = restTemplate.exchange(
                kafka + "/" + topic + "/" + count,
                HttpMethod.PUT,
                null,
                Void.class
        );
        assertEquals(HttpStatusCode.valueOf(200), response.getStatusCode(), "Failed to push messages to Kafka");
    }

    public List<String> recKafka(String topic, Integer timeout){
        ResponseEntity<List<String>> response = restTemplate.exchange(
                kafka + "/" + topic + "/" + timeout,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<String>>() {}
        );
        assertEquals(HttpStatusCode.valueOf(200), response.getStatusCode(), "Failed to receive messages from Kafka");
        assertNotNull(response.getBody(), "No messages received from Kafka");
        List<String> messages = response.getBody();
        assertNotNull(messages, "No messages received from Kafka");
        assertFalse(messages.isEmpty(), "No messages received from Kafka");

        return messages;
    }

    public void pushKafka(String topic, String json){
        ResponseEntity<Void> response = restTemplate.exchange(
                kafka + "/sendMessage/" + topic,
                HttpMethod.POST,
                new HttpEntity<>(json, PacketGenerator.createJsonHeaders()),
                Void.class
        );
        assertEquals(HttpStatusCode.valueOf(200), response.getStatusCode(), "Failed to push messages to Kafka");
    }

    public void procMsg(ProcessRequest request){
        ResponseEntity<Void> response = restTemplate.exchange(
                base + "/processMessages",
                HttpMethod.POST,
                new HttpEntity<>(request),
                Void.class
        );
        assertEquals(HttpStatusCode.valueOf(200), response.getStatusCode(), "Failed to process messages");
    }

    public String recCache(String key){
        ResponseEntity<String> response = restTemplate.exchange(
                cache + "/" + key,
                HttpMethod.GET,
                null,
                String.class
        );
        assertEquals(HttpStatusCode.valueOf(200), response.getStatusCode(), "Failed to receive cache");
        return Parser.parseString(response.getBody());
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

    public List<cacheEntry> clearCacheEntries(List<TransformMessage> messages, CacheService cacheService){
        List<cacheEntry> entries = new ArrayList<>();
        Set<String> keys = new HashSet<>();
        for (TransformMessage message : messages){
            String key = message.key;
            if (cacheService.checkKey(key)){
                keys.add(key);
            }
        }
        for (String key: keys){
            cacheEntry entry = new cacheEntry();
            entry.key = key;
            entry.entry = cacheService.retrieveFromCache(key);
            cacheService.removeFromCache(key);
        }
        return entries;
    }

}
