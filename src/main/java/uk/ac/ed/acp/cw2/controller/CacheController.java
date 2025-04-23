package uk.ac.ed.acp.cw2.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.acp.cw2.service.CacheService;

/**
 * Controller class responsible for handling REST endpoints for managing
 * cache storage using Redis. Provides functionality to retrieve and store
 * key-value pairs in the cache.
 */
@RestController
@RequestMapping("/cache")
public class CacheController {

    private static final Logger logger = LoggerFactory.getLogger(CacheController.class);
    private final CacheService cacheService;

    public CacheController(CacheService cacheService) {
        this.cacheService = cacheService;
    }

    @GetMapping("/{cacheKey}")
    public String retrieveFromCache(@PathVariable String cacheKey) {
        logger.info("Retrieving from cache: {}", cacheKey);
        return cacheService.retrieveFromCache(cacheKey);
    }

    @PutMapping("/{cacheKey}/{cacheValue}")
    public void storeInCache(@PathVariable String cacheKey, @PathVariable String cacheValue) {
        logger.info("Storing in cache: {} = {}", cacheKey, cacheValue);
        cacheService.storeInCache(cacheKey, cacheValue);
    }
}
