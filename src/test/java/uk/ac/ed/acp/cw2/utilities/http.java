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
import java.util.List;

import static org.apache.commons.lang3.math.NumberUtils.min;
import static org.junit.jupiter.api.Assertions.*;

public class http {

    private final String base;
    private final String rabbit;
    private final String kafka;
    private final String cache;
    private final TestRestTemplate restTemplate;


    public http(TestRestTemplate restTemplate, String baseUrl){
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

    public long timeRabbit(String queueName, Integer timeoutInMsec){
        long startTime = System.currentTimeMillis();
        ResponseEntity<List<String>> response = restTemplate.exchange(
                rabbit + "/" + queueName + "/" + timeoutInMsec,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<String>>() {}
        );
        assertEquals(HttpStatusCode.valueOf(200), response.getStatusCode(), "Failed to receive messages from Rabbit");
        long endTime = System.currentTimeMillis();
        return endTime-startTime;
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

    
    public boolean checkRabbitResponse(String queue, Integer timeout){
        long startTime = System.currentTimeMillis();
        ResponseEntity<List<String>> response = restTemplate.exchange(
                rabbit + "/" + queue + "/" + timeout,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<String>>() {}
        );
        long endTime = System.currentTimeMillis();
        long timeTaken = endTime - startTime;
        double low_thresh = timeout * 0.15;
        long threshold = min((long) low_thresh, 200);
        if (timeTaken - timeout > threshold){
            System.out.println("Timeout: " + timeout);
            return false;
        }
        assertEquals(HttpStatusCode.valueOf(200), response.getStatusCode(), "Failed to receive messages from Queue");
        try{
            List<String> messages = response.getBody();
            return !messages.isEmpty();
        } catch (Exception e){
            System.out.println("Error checking Rabbit response: " + e.getMessage());
            return false;
        }
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

    public long timeKafka(String topic, Integer timeout){
        long startTime = System.currentTimeMillis();
        ResponseEntity<List<String>> response = restTemplate.exchange(
                kafka + "/" + topic + "/" + timeout,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<String>>() {}
        );
        long endTime = System.currentTimeMillis();
        assertEquals(HttpStatusCode.valueOf(200), response.getStatusCode(), "Failed to receive messages from Kafka");
        return endTime-startTime;
    }

    public boolean checkKafkaResponse(String topic, Integer timeout){
        long startTime = System.currentTimeMillis();
        ResponseEntity<List<String>> response = restTemplate.exchange(
                kafka + "/" + topic + "/" + timeout,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<String>>() {}
        );
        long timeTaken = System.currentTimeMillis() - startTime;
        assertEquals(HttpStatusCode.valueOf(200), response.getStatusCode(), "Failed to receive messages from Kafka");
        double low_thresh = timeout * 0.15;
        long threshold = min((long) low_thresh, 200);
        if (timeTaken - timeout > threshold){
            System.out.println("Timeout: " + timeout);
            return false;
        }
        try{
            List<String> messages = response.getBody();
            return !messages.isEmpty();
        } catch (Exception e){
            System.out.println("Error checking Kafka response: " + e.getMessage());
            return false;
        }
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

    public List<String> clearCacheEntries(List<TransformMessage> messages, CacheService cacheService){
        List<String> entries = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        for (TransformMessage message : messages){
            String key = message.key;
            if (cacheService.checkKey(key)){
                keys.add(key);
            }
        }
        for (String key: keys){
            if (cacheService.checkKey(key)){
                entries.add(cacheService.retrieveFromCache(key));
                cacheService.removeFromCache(key);
            }
        }
        return entries;
    }

    public int getQueueMessageCount(String queueName) {
        ResponseEntity<Integer> response = restTemplate.exchange(
                rabbit + "/" + queueName + "/count",
                HttpMethod.GET,
                null,
                Integer.class
        );
        assertEquals(HttpStatusCode.valueOf(200), response.getStatusCode(), "Failed to get queue message count");
        return response.getBody();
    }

    public List<String> getURL(String url){
        ResponseEntity<List<String>> response = restTemplate.exchange(
                base + url,
                HttpMethod.GET,
                null,
                new ParameterizedTypeReference<List<String>>() {}
        );
        assertEquals(HttpStatusCode.valueOf(200), response.getStatusCode(), "Failed to get string");
        return response.getBody();
    }

}
