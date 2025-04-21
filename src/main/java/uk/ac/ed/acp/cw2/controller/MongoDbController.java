package uk.ac.ed.acp.cw2.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.acp.cw2.service.MongoDbService;

/**
 * Controller class responsible for handling REST endpoints for managing
 * cache storage using Redis. Provides functionality to retrieve and store
 * key-value pairs in the cache.
 */
@RestController
@RequestMapping("/mongodb")
public class MongoDbController {

    private static final Logger logger = LoggerFactory.getLogger(MongoDbController.class);
    private final MongoDbService mongoDbService;

    public MongoDbController(MongoDbService mongoDbService) {
        this.mongoDbService = mongoDbService;
    }


    @GetMapping("/cache/{cacheKey}")
    public String retrieveFromCache(@PathVariable String cacheKey) {
        return mongoDbService.retrieveFromCache(cacheKey);
    }

    @PutMapping("/cache/{cacheKey}/{cacheValue}")
    public void storeInCache(@PathVariable String cacheKey, @PathVariable String cacheValue) {
        mongoDbService.storeInCache(cacheKey, cacheValue);
    }

}
