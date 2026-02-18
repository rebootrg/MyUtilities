package com.open.async2.utility;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.IntStream;

public class BatchRunner {
    private static final int TOTAL_RECORDS = 100_000;
    private static final int BATCH_SIZE = 10_000;
    private static final int CONCURRENCY = 100; // Simultaneous requests within one batch

    public static void main(String[] args) {
        RecordProcessorService service = new RecordProcessorService(CONCURRENCY);
        List<RecordProcessorService.ProcessResult> finalResults = Collections.synchronizedList(new ArrayList<>());

        System.out.println("🚀 Starting Batch Processing...");
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < TOTAL_RECORDS; i += BATCH_SIZE) {
            int start = i;
            int end = Math.min(i + BATCH_SIZE, TOTAL_RECORDS);
            
            System.out.printf("📦 Processing Batch: %d to %d...%n", start, end);
            
            // Process the 10k batch
            List<RecordProcessorService.ProcessResult> batchResults = processBatch(service, start, end);
            
            // Add batch results to master list
            finalResults.addAll(batchResults);
            
            System.out.printf("✅ Batch Complete. Total collected so far: %d%n", finalResults.size());
        }

        long duration = (System.currentTimeMillis() - startTime) / 1000;
        System.out.printf("🏁 All Finished! Processed %d records in %ds%n", finalResults.size(), duration);
        
        service.shutdown();
    }

    private static List<RecordProcessorService.ProcessResult> processBatch(RecordProcessorService service, int start, int end) {
        List<CompletableFuture<RecordProcessorService.ProcessResult>> futures = IntStream.range(start, end)
                .mapToObj(i -> service.process("ID-" + i, "{\"val\":" + i + "}"))
                .toList();

        // Join here blocks the loop until all 10,000 (and their retries) are done
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();

        // Extract and return results
        return futures.stream()
                .map(CompletableFuture::join)
                .toList();
    }
}
