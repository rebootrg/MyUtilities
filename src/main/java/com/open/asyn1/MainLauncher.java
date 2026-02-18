package com.open.async.utility;

import java.time.Duration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;

public class MainLauncher {
    private static final int TOTAL_RECORDS = 10_000;
    private static final int CHUNK_SIZE = 5_000;
    private static final int CONCURRENT_THREADS = 100;

    public static void main(String[] args) {
        RecordProcessor processor = new RecordProcessor(CONCURRENT_THREADS);
        
        // Thread-safe collection to hold all 100k responses
        Queue<String> allResponses = new ConcurrentLinkedQueue<>();

        List<String> allRecords = IntStream.rangeClosed(1, TOTAL_RECORDS)
                .mapToObj(i -> "{\"id\":" + i + "}")
                .toList();

        System.out.println("Processing...");
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < allRecords.size(); i += CHUNK_SIZE) {
            int end = Math.min(i + CHUNK_SIZE, allRecords.size());
            List<String> chunk = allRecords.subList(i, end);

            // Process chunk and collect responses
            processAndCollect(processor, chunk, allResponses);
            
            System.out.printf("Progress: %d/%d collected.%n", allResponses.size(), TOTAL_RECORDS);
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Done! Collected " + allResponses.size() + " responses.");
        System.out.println("Total Time: " + (endTime - startTime) / 1000 + "s");

        // Example: Print first 5 responses
        allResponses.stream().limit(5).forEach(System.out::println);

        processor.shutdown();
    }

    private static void processAndCollect(RecordProcessor processor, List<String> chunk, Queue<String> results) {
        List<CompletableFuture<Void>> futures = chunk.stream()
                .map(record -> processor.sendWithRetry(record, 3, Duration.ofSeconds(1))
                        .thenAccept(results::add)) // Capture the result into the queue
                .toList();

        // Wait for this chunk to finish so we don't overload RAM
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
    }
}
