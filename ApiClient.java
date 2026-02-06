package com.open.rest.utility;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.open.rest.utility.PojoBatchProcessor.MyRequestPayload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class ApiClient {
	private static final Logger log = LoggerFactory.getLogger(ApiClient.class);

	private final HttpClient httpClient;
	private final ObjectMapper mapper;

	private static final int MAX_RETRIES = 3;
	private static final long INITIAL_BACKOFF_MS = 1000; // 1 second

	public ApiClient(HttpClient httpClient, ObjectMapper mapper) {
		this.httpClient = httpClient;
		this.mapper = mapper;
	}

	/**
	 * Executes the POST request with exponential backoff. Returns a MyResponsePojo
	 * on success, or null if all retries fail.
	 */
	public MyResponsePojo executeWithRetry(MyRequestPayload payload) {
		int attempt = 0;

		while (attempt <= MAX_RETRIES) {
			try {
				// 1. Serialize POJO to JSON
				String requestBody = mapper.writeValueAsString(payload);

				// 2. Build Request
				HttpRequest request = HttpRequest.newBuilder().uri(URI.create("https://api.example.com/v1/data"))
						.header("Content-Type", "application/json").header("Accept", "application/json")
						.timeout(Duration.ofSeconds(30)) // Response timeout
						.POST(HttpRequest.BodyPublishers.ofString(requestBody)).build();

				// 3. Execute
				HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

				// 4. Handle Response
				if (isSuccess(response.statusCode())) {
					MyResponsePojo result = mapper.readValue(response.body(), MyResponsePojo.class);
					result.id = payload.id; // Map original ID back to response
					return result;
				} else if (response.statusCode() >= 500) {
					log.warn("Server error {} for ID {}. Attempt {}/{}", response.statusCode(), payload.id, attempt + 1,
							MAX_RETRIES + 1);
				} else {
					log.error("Client error {} for ID {}. Skipping retries.", response.statusCode(), payload.id);
					break; // Don't retry 4xx errors
				}

			} catch (Exception e) {
				log.warn("Network error for ID {}: {}. Attempt {}/{}", payload.id, e.getMessage(), attempt + 1,
						MAX_RETRIES + 1);
			}

			attempt++;
			if (attempt <= MAX_RETRIES) {
				applyBackoff(attempt);
			}
		}
		return null;
	}

	private boolean isSuccess(int statusCode) {
		return statusCode >= 200 && statusCode < 300;
	}

	private void applyBackoff(int attempt) {
		try {
			// Exponential: 1s, 2s, 4s...
			long sleepTime = INITIAL_BACKOFF_MS * (long) Math.pow(2, attempt - 1);
			Thread.sleep(sleepTime);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
}