package com.arian.dqe.mockapi;

import com.arian.dqe.infra.AppProps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mock de la API interna con fallos inyectables (entorno dev del PDF). Vigila el invariante
 * "1 en vuelo por proveedor": si lo ve roto lo loguea y lo expone en /stats.
 */
@RestController
@Profile("mockapi")
public class MockApiController {

	private static final Logger log = LoggerFactory.getLogger(MockApiController.class);

	private final AppProps.MockApi cfg;
	private final Map<String, AtomicInteger> inflight = new ConcurrentHashMap<>();
	private final Map<String, Integer> maxInflight = new ConcurrentHashMap<>();
	private final Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
	private volatile String chaos = "ok";

	public MockApiController(AppProps props) {
		this.cfg = props.mockapi();
	}

	@GetMapping("/providers/{providerId}/records/{recordId}")
	public ResponseEntity<String> query(@PathVariable String providerId, @PathVariable long recordId) throws InterruptedException {
		int now = inflight.computeIfAbsent(providerId, k -> new AtomicInteger()).incrementAndGet();
		maxInflight.merge(providerId, now, Math::max);
		if (now > 1) {
			log.error("CONCURRENCY VIOLATION provider={} inflight={}", providerId, now);
		}
		try {
			Thread.sleep(ThreadLocalRandom.current().nextInt(cfg.minLatencyMs(), cfg.maxLatencyMs() + 1));
			switch (chaos) {
				case "down" -> {
					return ResponseEntity.status(503).body("{\"error\":\"down\"}");
				}
				case "slow" -> Thread.sleep(60_000);
				default -> {
				}
			}
			int n = calls.computeIfAbsent(providerId, k -> new AtomicInteger()).incrementAndGet();
			Integer fault = cfg.faults().get(providerId);
			if (fault != null && fault == 429 && n % 3 != 0) {
				return ResponseEntity.status(429).header("Retry-After", String.valueOf(cfg.retryAfterSeconds())).body("");
			}
			if (fault != null && fault != 429) {
				return ResponseEntity.status(fault).body("{\"error\":" + fault + "}");
			}
			return ResponseEntity.ok("{\"recordId\":" + recordId + ",\"providerId\":\"" + providerId
					+ "\",\"taxId\":\"76.123.456-7\",\"balance\":" + ThreadLocalRandom.current().nextInt(100_000) + "}");
		}
		finally {
			inflight.get(providerId).decrementAndGet();
		}
	}

	@PostMapping("/chaos/{mode}")
	public Map<String, String> chaos(@PathVariable String mode) {
		if (!Map.of("ok", 1, "down", 1, "slow", 1).containsKey(mode)) {
			throw new IllegalArgumentException(mode);
		}
		chaos = mode;
		log.warn("chaos = {}", mode);
		return Map.of("chaos", mode);
	}

	@GetMapping("/stats")
	public Map<String, Integer> stats() {
		return maxInflight;
	}

	@GetMapping("/health")
	public ResponseEntity<String> health() {
		return "ok".equals(chaos) ? ResponseEntity.ok("ok") : ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(chaos);
	}
}
