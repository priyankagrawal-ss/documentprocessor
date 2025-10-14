package com.eyelevel.documentprocessor.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A service to simulate interaction with an external GX API.
 * In a real-world scenario, this would contain an HTTP client (e.g., RestTemplate or WebClient)
 * to make calls to the actual GX service.
 */
@Service
@Slf4j
public class GxApiClientService {

    // Using a ConcurrentHashMap to safely simulate a persistent, idempotent service.
    private final ConcurrentHashMap<String, Long> bucketNameToIdMap = new ConcurrentHashMap<>();
    // Using an AtomicLong to generate unique IDs for new buckets.
    private final AtomicLong idGenerator = new AtomicLong(1000); // Start IDs from 1000

    /**
     * Retrieves the ID for a given bucket name. If the bucket does not exist,
     * it simulates creating one and returns the new ID.
     * This method is designed to be idempotent.
     *
     * @param bucketName The name of the bucket (e.g., "bucket1" from the ZIP folder).
     * @return The unique Long ID for the bucket.
     */
    public Long getOrCreateBucket(String bucketName) {
        log.info("Requesting GxBucketId for bucket name: '{}'", bucketName);

        // computeIfAbsent is a thread-safe way to "get or create".
        // It guarantees the lambda is only executed once for a given key.
        Long bucketId = bucketNameToIdMap.computeIfAbsent(bucketName, name -> {
            long newId = idGenerator.getAndIncrement();
            log.warn("GX Bucket '{}' not found. Simulating creation with new GxBucketId: {}", name, newId);
            return newId;
        });

        log.info("Resolved GxBucketName '{}' to GxBucketId: {}", bucketName, bucketId);
        return bucketId;
    }
}