package com.open.rest.utility;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

public class PojoBatchProcessor {

	public static void main(String[] args) {
		PojoBatchProcessor processor  = new PojoBatchProcessor();
		List<MyRequestPayload> input = new ArrayList<MyRequestPayload>();
		
		for(int i=0 ; i < 100 ; i++) {
			input.add(MyRequestPayload.builder().id(i).build());
		}
		processor.processInBatches(input);
	}
	
	
    // Thread-safe JSON mapper
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_RETRIES = 3;
    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(200);
    
    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .executor(EXECUTOR)
            .build();

    // Example POJO
    @Getter
    @Setter
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class MyRequestPayload {
        public int id;
        public String status;
        public long timestamp;

        public MyRequestPayload(int id) {
            this.id = id;
            this.status = "PENDING";
            this.timestamp = System.currentTimeMillis();
        }
    }

    public void processInBatches(List<MyRequestPayload> allPayloads) {
        int totalSize = allPayloads.size();
        int batchSize = 1000;

        for (int i = 0; i < totalSize; i += batchSize) {
            int end = Math.min(i + batchSize, totalSize);
            List<MyRequestPayload> batch = allPayloads.subList(i, end);

            System.out.printf("Processing Batch: %d to %d%n", i, end);
            
            // Map POJOs to Async Tasks
            CompletableFuture<?>[] futures = batch.stream()
                .map(payload -> CompletableFuture.runAsync(() -> executeWithRetry(payload), EXECUTOR))
                .toArray(CompletableFuture[]::new);

            // Wait for this batch of 1000 to finish
            CompletableFuture.allOf(futures).join();
        }
        EXECUTOR.shutdown();
    }

    private void executeWithRetry(MyRequestPayload payload) {
        int attempts = 0;
        while (attempts <= MAX_RETRIES) {
            try {
                // 1. Convert POJO to JSON string
                String jsonBody = MAPPER.writeValueAsString(payload);

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("https://api.example.com/data"))
                        .header("Content-Type", "application/json")
                        .timeout(Duration.ofSeconds(30))
                        .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                        .build();

                HttpResponse<String> response = CLIENT.send(request, HttpResponse.BodyHandlers.ofString());

                if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    return; // Success
                }

                // Retry only on Server Errors (5xx)
                if (response.statusCode() < 500) break; 

            } catch (Exception e) {
                System.err.println("Attempt " + (attempts + 1) + " failed for ID " + payload.id);
            }

            attempts++;
            if (attempts <= MAX_RETRIES) {
                backoff(attempts);
            }
        }
    }

    private void backoff(int attempt) {
        try {
            // Exponential backoff: 1s, 2s, 4s...
            Thread.sleep((long) Math.pow(2, attempt - 1) * 1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}