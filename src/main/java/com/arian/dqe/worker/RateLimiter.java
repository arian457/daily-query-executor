package com.arian.dqe.worker;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * Tasa objetivo con jitter: cada permiso sale a intervalo 1/rate más un jitter uniforme
 * en [0, 1/rate). Sin ráfagas (D5). La tasa se lee en cada acquire para honrar cambios en SSM.
 */
public class RateLimiter {

	private final DoubleSupplier ratePerSecond;
	private long nextFreeNanos = System.nanoTime();

	public RateLimiter(DoubleSupplier ratePerSecond) {
		this.ratePerSecond = ratePerSecond;
	}

	public void acquire() throws InterruptedException {
		long wait;
		synchronized (this) {
			long interval = (long) (1_000_000_000L / Math.max(ratePerSecond.getAsDouble(), 0.001));
			long now = System.nanoTime();
			long slot = Math.max(now, nextFreeNanos);
			nextFreeNanos = slot + interval;
			wait = slot - now + ThreadLocalRandom.current().nextLong(interval);
		}
		if (wait > 0) {
			Thread.sleep(wait / 1_000_000, (int) (wait % 1_000_000));
		}
	}
}
