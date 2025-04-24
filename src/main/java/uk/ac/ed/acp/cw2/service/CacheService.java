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
public class CacheService {
    private static final Logger logger = LoggerFactory.getLogger(CacheService.class);
    private final RuntimeEnvironment environment;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public CacheService(RuntimeEnvironment environment) {
        this.environment = environment;
    }

    public Boolean checkKey(String key){
        try (JedisPool pool = new JedisPool(environment.getRedisHost(), environment.getRedisPort()); Jedis jedis = pool.getResource()) {
            logger.debug("Checking {}", key);
            return (jedis.exists(key));
        }
    }

    public String retrieveFromCache(String cacheKey) {
        logger.debug(String.format("Retrieving %s from cache", cacheKey));
        try (JedisPool pool = new JedisPool(environment.getRedisHost(), environment.getRedisPort()); Jedis jedis = pool.getResource()) {
            logger.debug("Redis connection established");

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

    public void storeInCache(String cacheKey, ObjectNode cacheValue) {
        try {
            String jsonString = objectMapper.writeValueAsString(cacheValue);
            storeInCache(cacheKey, jsonString);
        } catch (Exception e) {
            logger.error("Error converting ObjectNode to JSON string: {}", e.getMessage());
        }
    }

    public void removeFromCache(String cacheKey){
        try (JedisPool pool = new JedisPool(environment.getRedisHost(), environment.getRedisPort()); Jedis jedis = pool.getResource()) {
            jedis.del(cacheKey);
        } catch (Exception e) {
            logger.error(e.getMessage());
            throw e;
        }
    }
}
