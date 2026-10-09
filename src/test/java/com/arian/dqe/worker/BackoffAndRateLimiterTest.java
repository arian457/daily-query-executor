package com.arian.dqe.worker;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BackoffAndRateLimiterTest {

	@Test
	void backoffIsFullJitterCappedAt15Minutes() {
		for (int i = 0; i < 500; i++) {
			assertTrue(Backoff.seconds(1, 30, 900) <= 60);
			assertTrue(Backoff.seconds(4, 30, 900) <= 480);   // 4 intentos → máximo 8 min (PDF)
			assertTrue(Backoff.seconds(10, 30, 900) <= 900);  // tope = DelaySeconds máximo de SQS
		}
		assertEquals(0, Backoff.seconds(3, 0, 900));
	}

	@Test
	void rateLimiterHoldsTargetRateWithJitter() throws InterruptedException {
		RateLimiter limiter = new RateLimiter(() -> 50.0); // 20 ms por permiso + jitter [0, 20 ms)
		long start = System.nanoTime();
		for (int i = 0; i < 25; i++) {
			limiter.acquire();
		}
		long elapsedMs = (System.nanoTime() - start) / 1_000_000;
		assertTrue(elapsedMs >= 480, "muy rápido: " + elapsedMs + " ms");   // 24 intervalos × 20 ms
		assertTrue(elapsedMs <= 1200, "muy lento: " + elapsedMs + " ms");
	}
}
