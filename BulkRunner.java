package com.open.async2.utility;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BulkRunner {
    public static void main(String[] args) throws InterruptedException {
        int totalRecords = 100_000;
        int concurrencyLimit = 100;
        
        RecordProcessorService service = new RecordProcessorService(concurrencyLimit);
        
        // 1. Thread-safe collection for results
        Queue<RecordProcessorService.ProcessResult> resultCollector = new ConcurrentLinkedQueue<>();
        
        // 2. Throttle submission to prevent OOM
        Semaphore submissionThrottle = new Semaphore(concurrencyLimit);
        CountDownLatch globalLatch = new CountDownLatch(totalRecords);
        AtomicInteger successCount = new AtomicInteger(0);

        System.out.println("Processing 100k records...");

        for (int i = 0; i < totalRecords; i++) {
            final String id = "REC-" + i;
            submissionThrottle.acquire(); // Wait if we already have 100 active requests

            service.process(id, "{\"id\":" + i + "}")
                .thenAccept(result -> {
                    if (result.success()) successCount.incrementAndGet();
                    resultCollector.add(result); // Capture response
                })
                .whenComplete((res, ex) -> {
                    submissionThrottle.release();
                    globalLatch.countDown();
                });

            if (i % 5000 == 0) System.out.println("Submitted: " + i);
        }

        // Wait for final completion
        globalLatch.await(1, TimeUnit.HOURS);
        
        System.out.println("Finished! Collected: " + resultCollector.size());
        System.out.println("Successes: " + successCount.get());
        
        service.shutdown();
    }
}
