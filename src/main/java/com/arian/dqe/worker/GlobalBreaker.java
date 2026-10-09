package com.arian.dqe.worker;

import com.arian.dqe.infra.AppProps;
import com.arian.dqe.infra.Params;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

/**
 * Breaker global por correlación: > failRatio de fallos y ≥ minProviders proveedores fallando
 * en la ventana → abre en SSM; la carga horaria difiere y los workers dejan de recibir.
 * Una sonda periódica contra la API decide cuándo cerrar.
 * ponytail: ventana en memoria por instancia; con varios workers y señal dispar va a CloudWatch metric math (D7).
 */
@Component
@Profile("worker")
public class GlobalBreaker {

	private static final Logger log = LoggerFactory.getLogger(GlobalBreaker.class);

	private record Sample(long at, String providerId, boolean ok) {
	}

	private final Params params;
	private final AppProps.Breaker cfg;
	private final ApiClient api;
	private final Clock clock;
	private final Deque<Sample> window = new ArrayDeque<>();

	@Autowired
	public GlobalBreaker(Params params, AppProps props, ApiClient api) {
		this(params, props, api, Clock.systemUTC());
	}

	GlobalBreaker(Params params, AppProps props, ApiClient api, Clock clock) {
		this.params = params;
		this.cfg = props.breaker();
		this.api = api;
		this.clock = clock;
	}

	public boolean isOpen() {
		return params.globalBreakerOpen();
	}

	public synchronized void record(String providerId, boolean ok) {
		long now = clock.instant().getEpochSecond();
		window.addLast(new Sample(now, providerId, ok));
		while (!window.isEmpty() && window.peekFirst().at() < now - cfg.globalWindowSeconds()) {
			window.pollFirst();
		}
		if (window.size() < cfg.globalMinSamples() || params.globalBreakerOpen()) {
			return;
		}
		int failures = 0;
		Set<String> failing = new HashSet<>();
		for (Sample s : window) {
			if (!s.ok()) {
				failures++;
				failing.add(s.providerId());
			}
		}
		if ((double) failures / window.size() > cfg.globalFailRatio() && failing.size() >= cfg.globalMinProviders()) {
			log.warn("breaker global ABIERTO: {}/{} fallos, {} proveedores", failures, window.size(), failing.size());
			params.setGlobalBreaker(true);
			window.clear();
		}
	}

	@Scheduled(fixedDelayString = "#{${app.breaker.global-probe-seconds} * 1000}")
	public void probe() {
		if (!params.globalBreakerOpen()) {
			return;
		}
		if (api.healthy()) {
			log.info("breaker global CERRADO: la API responde");
			params.setGlobalBreaker(false);
		}
	}
}
