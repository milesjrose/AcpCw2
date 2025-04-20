package uk.ac.ed.acp.cw2.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

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
        logger.info(String.format("Storing %s in cache with key %s", cacheValue, cacheKey));
        try (JedisPool pool = new JedisPool(environment.getRedisHost(), environment.getRedisPort()); Jedis jedis = pool.getResource()) {
            jedis.set(cacheKey, cacheValue);
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw e;
        }
    }

    public void storeInCache(String cacheKey, ObjectNode cacheValue) {
        try {
            // Convert ObjectNode to JSON string
            String jsonString = objectMapper.writeValueAsString(cacheValue);
            // Store the JSON string in cache
            storeInCache(cacheKey, jsonString);
        } catch (Exception e) {
            logger.error("Error converting ObjectNode to JSON string: {}", e.getMessage());
            throw new RuntimeException("Failed to store ObjectNode in cache", e);
        }
    }
}
