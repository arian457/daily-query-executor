package com.arian.dqe.infra;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;
import java.util.Map;

@ConfigurationProperties(prefix = "app")
public record AppProps(
		Api api,
		Queues queues,
		Tables tables,
		String bucket,
		Loader loader,
		Retry retry,
		Breaker breaker,
		Ssm ssm,
		Limits limits,
		MockApi mockapi,
		List<String> maskFields) {

	public record Api(String baseUrl, int timeoutSeconds, String endpointAllowlist) {
	}

	public record Queues(String tasks, String dlq, String retry, int visibilitySeconds, int maxReceiveCount) {
	}

	public record Tables(String plan, String execution, String breaker) {
	}

	public record Loader(String cron, int slots) {
	}

	public record Retry(int maxAttempts, int baseSeconds, int capSeconds) {
	}

	public record Breaker(int providerThreshold, int providerPauseSeconds, int providerMaxPauseSeconds,
			double globalFailRatio, int globalMinProviders, int globalMinSamples, int globalWindowSeconds,
			int globalProbeSeconds) {
	}

	public record Ssm(String globalBreaker, String maxConcurrent, String rate) {
	}

	/** Valores iniciales de los parámetros SSM; después se cambian en SSM, no acá. */
	public record Limits(int maxConcurrentProviders, int ratePerSecond) {
	}

	public record MockApi(int minLatencyMs, int maxLatencyMs, int retryAfterSeconds, Map<String, Integer> faults) {
	}
}
