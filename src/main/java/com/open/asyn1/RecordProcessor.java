package com.open.async.utility;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RecordProcessor {
    private final ExecutorService executor;
    private final HttpClient httpClient;

    public RecordProcessor(int threadCount) {
        this.executor = Executors.newFixedThreadPool(threadCount);
        this.httpClient = HttpClient.newBuilder()
                .executor(executor)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    // Now returns CompletableFuture<String> to capture the response body
    public CompletableFuture<String> sendWithRetry(String data, int retries, Duration delay) {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://httpbin.org/post"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(data))
                .build();

        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenCompose(response -> {
                    int status = response.statusCode();
                    if (status >= 200 && status < 300) {
                        return CompletableFuture.completedFuture("SUCCESS: " + response.body());
                    } else if (status == 429 || (status >= 500 && status <= 504)) {
                        return CompletableFuture.<String>failedFuture(new RuntimeException("Retryable: " + status));
                    } else {
                        return CompletableFuture.completedFuture("FATAL_ERROR: " + status);
                    }
                })
                .exceptionallyCompose(ex -> {
                    if (retries > 0) {
                        Executor delayed = CompletableFuture.delayedExecutor(delay.toSeconds(), TimeUnit.SECONDS, executor);
                        return CompletableFuture.runAsync(() -> {}, delayed)
                                .thenCompose(v -> sendWithRetry(data, retries - 1, delay.multipliedBy(2)));
                    } else {
                        return CompletableFuture.completedFuture("EXHAUSTED_RETRIES: " + data);
                    }
                });
    }

    public void shutdown() {
        executor.shutdown();
    }
}
