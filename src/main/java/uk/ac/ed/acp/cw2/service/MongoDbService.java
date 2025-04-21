package uk.ac.ed.acp.cw2.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import java.util.Random;

/**
 * Service class responsible for managing cache storage using Redis.
 * Provides functionality to retrieve and store key-value pairs in the cache.
 */
@Service
public class MongoDbService {
    private static final Logger logger = LoggerFactory.getLogger(MongoDbService.class);
    private final RuntimeEnvironment environment;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public MongoDbService(RuntimeEnvironment environment) {
        this.environment = environment;
    }

    public String retrieveFromCache(String cacheKey) {
        logger.info(String.format("Retrieving %s from cache", cacheKey));
        try (JedisPool pool = new JedisPool(environment.getRedisHost(), environment.getRedisPort()); Jedis jedis = pool.getResource()) {
            logger.info("Redis connection established");

            String result = null;
            if (jedis.exists(cacheKey)) {
                result = jedis.get(cacheKey);
            }
            return result;
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public void storeInCache(String cacheKey, String cacheValue) {
        logger.debug(String.format("Storing %s in cache with key %s", cacheValue, cacheKey));
        try (JedisPool pool = new JedisPool(environment.getRedisHost(), environment.getRedisPort()); Jedis jedis = pool.getResource()) {
            jedis.set(cacheKey, cacheValue);
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public String storeInCache(ObjectNode cacheValue) {
        String key = generateRandomKey();
        try {
            String jsonString = objectMapper.writeValueAsString(cacheValue);
            storeInCache(key, jsonString);
        } catch (Exception e) {
            logger.error("Error converting ObjectNode to JSON string: {}", e.getMessage());
            throw new RuntimeException("Failed to store ObjectNode in cache", e);
        }
        return key;
    }

    
    /**
     * Generates a random 10-character key in the format "12345-xxxxx"
     * where the first part is numeric and the second part is alphanumeric
     * 
     * @return a random 10-character key
     */
    public String generateRandomKey() {
        // Generate a random 5-digit number for the first part
        int numericPart = 10000 + new Random().nextInt(90000);
        
        // Generate a random 5-character alphanumeric string for the second part
        String alphanumericChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder alphanumericPart = new StringBuilder();
        Random random = new Random();
        
        for (int i = 0; i < 5; i++) {
            int index = random.nextInt(alphanumericChars.length());
            alphanumericPart.append(alphanumericChars.charAt(index));
        }
        
        // Combine the parts with a hyphen
        return numericPart + "-" + alphanumericPart.toString();
    }
}
