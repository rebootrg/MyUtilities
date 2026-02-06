import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class ProductionBatchProcessor {
    private static final Logger log = LoggerFactory.getLogger(ProductionBatchProcessor.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    private final ExecutorService executor;
    private final HttpClient httpClient;
    private final OracleDatabaseService dbService;
    private final ApiClient apiClient;

    private static final int BATCH_SIZE = 1000;

    public ProductionBatchProcessor(int threadCount) {
        // Naming threads helps in profiling and stack traces
        ThreadFactory threadFactory = r -> {
            Thread t = new Thread(r);
            t.setName("api-worker-" + t.getId());
            return t;
        };

        this.executor = Executors.newFixedThreadPool(threadCount, threadFactory);
        this.httpClient = HttpClient.newBuilder()
                .executor(executor)
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        
        this.dbService = new OracleDatabaseService();
        this.apiClient = new ApiClient(httpClient, MAPPER);
    }

    public void runProcess(List<MyRequestPayload> allPayloads) {
        log.info("Starting production processing for {} records", allPayloads.size());
        dbService.initializeTempTable();

        try {
            for (int i = 0; i < allPayloads.size(); i += BATCH_SIZE) {
                int end = Math.min(i + BATCH_SIZE, allPayloads.size());
                List<MyRequestPayload> batch = allPayloads.subList(i, end);

                processBatch(batch);
                
                log.info("Completed batch {}/{}", (i / BATCH_SIZE) + 1, (allPayloads.size() / BATCH_SIZE));
            }
        } finally {
            shutdown();
        }
    }

    private void processBatch(List<MyRequestPayload> batch) {
        // Parallel execution of API calls
        List<CompletableFuture<MyResponsePojo>> futures = batch.stream()
                .map(payload -> CompletableFuture.supplyAsync(() -> apiClient.executeWithRetry(payload), executor))
                .collect(Collectors.toList());

        // Wait for all in batch
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // Collect successful responses
        List<MyResponsePojo> results = futures.stream()
                .map(f -> f.getNow(null))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        // Batch save to Database
        if (!results.isEmpty()) {
            dbService.upsertResponses(results);
        }
    }

    private void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("Engine shutdown complete.");
    }
}
