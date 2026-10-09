package com.arian.dqe.infra;

import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.ParameterAlreadyExistsException;
import software.amazon.awssdk.services.ssm.model.ParameterType;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Parámetros operativos en SSM (D8): se cambian sin deploy. Lectura cacheada 10 s.
 */
@Component
public class Params {

	public static final String OPEN = "open";
	public static final String CLOSED = "closed";

	private final SsmClient ssm;
	private final AppProps.Ssm names;
	private final Map<String, Cached> cache = new ConcurrentHashMap<>();

	private record Cached(String value, Instant at) {
	}

	public Params(SsmClient ssm, AppProps props) {
		this.ssm = ssm;
		this.names = props.ssm();
	}

	public void ensureDefaults(int maxConcurrent, int ratePerSecond) {
		putIfAbsent(names.globalBreaker(), CLOSED);
		putIfAbsent(names.maxConcurrent(), String.valueOf(maxConcurrent));
		putIfAbsent(names.rate(), String.valueOf(ratePerSecond));
	}

	public boolean globalBreakerOpen() {
		return OPEN.equals(get(names.globalBreaker()));
	}

	public void setGlobalBreaker(boolean open) {
		put(names.globalBreaker(), open ? OPEN : CLOSED);
	}

	public int maxConcurrentProviders() {
		return Integer.parseInt(get(names.maxConcurrent()));
	}

	public double ratePerSecond() {
		return Double.parseDouble(get(names.rate()));
	}

	private String get(String name) {
		Cached c = cache.get(name);
		if (c != null && c.at.isAfter(Instant.now().minusSeconds(10))) {
			return c.value;
		}
		String v = ssm.getParameter(r -> r.name(name)).parameter().value();
		cache.put(name, new Cached(v, Instant.now()));
		return v;
	}

	private void put(String name, String value) {
		ssm.putParameter(r -> r.name(name).value(value).type(ParameterType.STRING).overwrite(true));
		cache.put(name, new Cached(value, Instant.now()));
	}

	private void putIfAbsent(String name, String value) {
		try {
			ssm.putParameter(r -> r.name(name).value(value).type(ParameterType.STRING).overwrite(false));
		}
		catch (ParameterAlreadyExistsException ignored) {
		}
	}
}
