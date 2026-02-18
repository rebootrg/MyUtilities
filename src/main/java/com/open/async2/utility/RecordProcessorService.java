package com.open.async2.utility;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.*;

public class RecordProcessorService {
    private final HttpClient httpClient;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final ExecutorService workerPool;

    public RecordProcessorService(int concurrency) {
        this.workerPool = Executors.newFixedThreadPool(concurrency);
        this.httpClient = HttpClient.newBuilder()
                .executor(workerPool)
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    public CompletableFuture<ProcessResult> process(String id, String payload) {
        return executeWithRetry(id, payload, 3, Duration.ofSeconds(1));
    }

    private CompletableFuture<ProcessResult> executeWithRetry(String id, String data, int attempts, Duration delay) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://httpbin.org/post"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(data))
                .timeout(Duration.ofSeconds(10))
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .handle((response, ex) -> {
                    if (ex != null || (response != null && isRetryable(response.statusCode()))) {
                        if (attempts > 0) return retry(id, data, attempts, delay);
                        return CompletableFuture.completedFuture(new ProcessResult(id, false, "Error: " + (ex != null ? ex.getMessage() : response.statusCode())));
                    }
                    return CompletableFuture.completedFuture(new ProcessResult(id, true, response.body()));
                })
                .thenCompose(future -> future);
    }

    private CompletableFuture<ProcessResult> retry(String id, String data, int attempts, Duration delay) {
        CompletableFuture<ProcessResult> promise = new CompletableFuture<>();
        scheduler.schedule(() -> {
            executeWithRetry(id, data, attempts - 1, delay.multipliedBy(2))
                .thenAccept(promise::complete);
        }, delay.toMillis(), TimeUnit.MILLISECONDS);
        return promise;
    }

    private boolean isRetryable(int status) {
        return status == 429 || (status >= 500 && status <= 504);
    }

    public void shutdown() {
        workerPool.shutdown();
        scheduler.shutdown();
    }

    public record ProcessResult(String id, boolean success, String responseData) {}
}
