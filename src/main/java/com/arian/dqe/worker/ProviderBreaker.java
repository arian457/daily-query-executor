package com.arian.dqe.worker;

import com.arian.dqe.infra.AppProps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Clock;
import java.util.Map;

/**
 * Breaker por proveedor, estado en DynamoDB. threshold fallos seguidos → pausa su grupo
 * pauseSeconds; al vencer, el siguiente mensaje actúa de sonda: éxito cierra, fallo duplica
 * la pausa (tope maxPauseSeconds). Pausar = extender visibility, lo hace el listener.
 */
@Component
public class ProviderBreaker {

	public record State(int failures, long openUntil, int pause) {
		static final State CLOSED = new State(0, 0, 0);

		boolean halfOpen(long now) {
			return pause > 0 && now >= openUntil;
		}
	}

	private final DynamoDbClient dynamo;
	private final String table;
	private final AppProps.Breaker cfg;
	private final Clock clock;

	@Autowired
	public ProviderBreaker(DynamoDbClient dynamo, AppProps props) {
		this(dynamo, props, Clock.systemUTC());
	}

	ProviderBreaker(DynamoDbClient dynamo, AppProps props, Clock clock) {
		this.dynamo = dynamo;
		this.table = props.tables().breaker();
		this.cfg = props.breaker();
		this.clock = clock;
	}

	public State state(String providerId) {
		Map<String, AttributeValue> item = dynamo.getItem(r -> r.tableName(table).key(key(providerId))).item();
		if (item == null || item.isEmpty()) {
			return State.CLOSED;
		}
		return new State((int) num(item, "failures"), num(item, "openUntil"), (int) num(item, "pause"));
	}

	/** Segundos que falta para que el proveedor pueda recibir; 0 si está cerrado o en sonda. */
	public int remainingPause(State s) {
		return (int) Math.max(0, s.openUntil() - now());
	}

	public void recordSuccess(String providerId, State s) {
		if (!s.equals(State.CLOSED)) {
			write(providerId, State.CLOSED);
		}
	}

	public State recordFailure(String providerId, State s) {
		long now = now();
		State next;
		if (s.halfOpen(now)) {
			int pause = Math.min(cfg.providerMaxPauseSeconds(), s.pause() * 2);
			next = new State(0, now + pause, pause);
		}
		else if (s.failures() + 1 >= cfg.providerThreshold()) {
			int pause = s.pause() > 0 ? s.pause() : cfg.providerPauseSeconds();
			next = new State(0, now + pause, pause);
		}
		else {
			next = new State(s.failures() + 1, s.openUntil(), s.pause());
		}
		write(providerId, next);
		return next;
	}

	private void write(String providerId, State s) {
		dynamo.putItem(r -> r.tableName(table).item(Map.of(
				"providerId", AttributeValue.fromS(providerId),
				"failures", AttributeValue.fromN(String.valueOf(s.failures())),
				"openUntil", AttributeValue.fromN(String.valueOf(s.openUntil())),
				"pause", AttributeValue.fromN(String.valueOf(s.pause())))));
	}

	private long now() {
		return clock.instant().getEpochSecond();
	}

	private static Map<String, AttributeValue> key(String providerId) {
		return Map.of("providerId", AttributeValue.fromS(providerId));
	}

	private static long num(Map<String, AttributeValue> item, String name) {
		AttributeValue v = item.get(name);
		return v == null ? 0 : Long.parseLong(v.n());
	}
}
