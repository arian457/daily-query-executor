package com.arian.dqe.worker;

import java.util.concurrent.ThreadLocalRandom;

/**
 * espera = random(0, min(cap, base × 2^intento)). Jitter completo; cap = 15 min = máximo DelaySeconds de SQS.
 */
public final class Backoff {

	private Backoff() {
	}

	public static int seconds(int attempt, int baseSeconds, int capSeconds) {
		long max = Math.min(capSeconds, (long) baseSeconds << attempt);
		return max <= 0 ? 0 : ThreadLocalRandom.current().nextInt((int) max + 1);
	}
}
